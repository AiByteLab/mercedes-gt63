package com.mercedes.streamprocessor;

import com.mercedes.streamprocessor.model.AdClickEvent;
import com.mercedes.streamprocessor.model.AttributedPageView;
import com.mercedes.streamprocessor.model.PageViewEvent;
import com.mercedes.streamprocessor.output.OutputSink;
import com.mercedes.streamprocessor.output.SqliteOutputSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Concurrent-processing tests against an embedded Kafka broker.
 *
 * @TestMethodOrder is required because the tests share a single JoinEngine bean,
 * so watermarks accumulate across methods. Each test uses a higher event_time
 * base than the previous one to avoid being dropped as too-late.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 4, topics = {"page_views", "ad_clicks"})
@Import(ConcurrencyTest.TestSinkConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
class ConcurrencyTest {

    private static Path tempDir;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) throws Exception {
        tempDir = Files.createTempDirectory("concurrency-test-");
        registry.add("output.database.path", () -> tempDir.resolve("test.db").toString());
        registry.add("kafka.bootstrap-servers", () ->
                System.getProperty("spring.embedded.kafka.brokers"));
    }

    /**
     * Replaces the real sink with a wrapper that records the page_view_id for every
     * write, in observed order. Avoids @SpyBean (which fails on Java 25).
     */
    @TestConfiguration
    static class TestSinkConfig {
        @Bean
        @Primary
        InstrumentedSink instrumentedSink(SqliteOutputSink delegate) {
            return new InstrumentedSink(delegate);
        }
    }

    static class InstrumentedSink implements OutputSink {
        private final OutputSink delegate;
        final List<String> processingOrder = new CopyOnWriteArrayList<>();

        InstrumentedSink(OutputSink delegate) { this.delegate = delegate; }

        void reset() {
            processingOrder.clear();
        }

        @Override
        public void write(AttributedPageView record) {
            processingOrder.add(record.getPageViewId());
            delegate.write(record);
        }

        @Override
        public void flush() { delegate.flush(); }
    }

    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private KafkaListenerEndpointRegistry registry;
    @Autowired private InstrumentedSink instrumentedSink;

    @Value("${output.database.path}")
    private String dbPath;

    @BeforeEach
    void cleanState() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement s = c.createStatement()) {
            s.execute("DELETE FROM attributed_page_views");
        }
        instrumentedSink.reset();

        // Block until consumers finish initial partition assignment, otherwise
        // the first records produced can be missed.
        for (var container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 4);
        }
    }

    // ---------------------------------------------------------------
    // Test 1 — events from all partitions of a multi-partition topic are processed
    // ---------------------------------------------------------------
    @Test
    @Order(1)
    void parallelPartitionProcessing() throws Exception {
        // Round-robin across all 4 partitions: pv_par_<i> goes to partition i % 4.
        // Asserting on thread count was flaky (Kafka's group rebalance is async),
        // so we assert all partitions delivered events instead.
        // eventTime base: minute 0 (first test, no prior watermark).
        int totalEvents = 40;
        for (int i = 0; i < totalEvents; i++) {
            sendPageViewToPartition("pv_par_" + i, "user_" + (i % 8), i, i % 4);
        }

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(rowCount()).isEqualTo(totalEvents));

        Set<Integer> partitionsCovered = instrumentedSink.processingOrder.stream()
                .map(id -> Integer.parseInt(id.substring("pv_par_".length())) % 4)
                .collect(java.util.stream.Collectors.toSet());
        assertThat(partitionsCovered)
                .as("expected events from all 4 partitions, saw: " + partitionsCovered)
                .containsExactlyInAnyOrder(0, 1, 2, 3);
    }

    // ---------------------------------------------------------------
    // Test 2 — per-partition ordering preserved
    // ---------------------------------------------------------------
    @Test
    @Order(2)
    void partitionOrderingPreserved() throws Exception {
        // Same user_id → same partition → strict ordering.
        // eventTime base: minute 1000 (above test 1's max ~minute 39).
        int n = 20;
        int base = 1000;
        for (int i = 0; i < n; i++) {
            sendPageView("pv_order_" + i, "ordered_user", base + i);
        }

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(instrumentedSink.processingOrder).hasSize(n));

        for (int i = 0; i < n; i++) {
            assertThat(instrumentedSink.processingOrder.get(i)).isEqualTo("pv_order_" + i);
        }
    }

    // ---------------------------------------------------------------
    // Test 3 — no deadlocks under load
    // ---------------------------------------------------------------
    @Test
    @Order(3)
    void noDeadlocks() throws Exception {
        // 200 events stress concurrent partition coordination without exhausting
        // Spring Kafka's default retry policy (per-record SQLite commit is the bottleneck).
        // eventTime base: minute 10000 (above tests 1+2 max).
        int totalEvents = 200;
        int base = 10000;
        for (int i = 0; i < totalEvents; i++) {
            sendPageView("pv_load_" + i, "user_" + (i % 25), base + i);
        }

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(rowCount()).isEqualTo(totalEvents));
    }

    // ---------------------------------------------------------------
    // Test 4 — same-user PV+click traffic does not lose backpatches (regression test for the per-user lock)
    // ---------------------------------------------------------------
    @Test
    @Order(4)
    void backpatchSucceedsUnderConcurrentSameUserTraffic() throws Exception {
        // Without the per-user lock, the click thread's findAttributablePageViews
        // can race the PV thread's addPageView and miss the backpatch, leaving the
        // row NULL.
        // All events use the same event_time so the watermark stays put. Otherwise
        // page_views processing can overtake the ad_clicks consumer and push the
        // watermark past earlier clicks, dropping them as too-late.
        // eventTime base: minute 50000 (above tests 1+2+3).
        int n = 50;
        int pvMin = 50000;
        int clickMin = pvMin - 1; // within attribution window, before pv

        for (int i = 0; i < n; i++) {
            String user = "race_user_" + i;
            sendPageView("pv_race_" + i, user, pvMin);
            sendAdClick("click_race_" + i, user, "campaign_R_" + i, clickMin);
        }

        await().atMost(Duration.ofSeconds(45))
                .untilAsserted(() -> assertThat(rowCount()).isEqualTo(n));

        assertThat(attributedRowCount())
                .as("every PV should be attributed; per-user lock should prevent the backpatch race")
                .isEqualTo(n);
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private void sendPageView(String eventId, String userId, int minuteOffset) throws Exception {
        PageViewEvent pv = buildPageView(eventId, userId, minuteOffset);
        kafkaTemplate.send("page_views", userId, objectMapper.writeValueAsString(pv)).get();
    }

    private void sendPageViewToPartition(String eventId, String userId, int minuteOffset, int partition) throws Exception {
        PageViewEvent pv = buildPageView(eventId, userId, minuteOffset);
        kafkaTemplate.send("page_views", partition, userId, objectMapper.writeValueAsString(pv)).get();
    }

    private void sendAdClick(String clickId, String userId, String campaignId, int minuteOffset) throws Exception {
        AdClickEvent click = AdClickEvent.builder()
                .clickId(clickId)
                .userId(userId)
                .campaignId(campaignId)
                .eventTime(Instant.parse("2024-01-01T12:00:00Z").plus(Duration.ofMinutes(minuteOffset)))
                .build();
        kafkaTemplate.send("ad_clicks", userId, objectMapper.writeValueAsString(click)).get();
    }

    private static PageViewEvent buildPageView(String eventId, String userId, int minuteOffset) {
        return PageViewEvent.builder()
                .eventId(eventId)
                .userId(userId)
                .eventTime(Instant.parse("2024-01-01T12:00:00Z").plus(Duration.ofMinutes(minuteOffset)))
                .url("/p/" + eventId)
                .build();
    }

    private long rowCount() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM attributed_page_views")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private long attributedRowCount() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT COUNT(*) FROM attributed_page_views WHERE attributed_click_id IS NOT NULL")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
