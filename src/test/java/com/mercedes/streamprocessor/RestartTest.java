package com.mercedes.streamprocessor;

import com.mercedes.streamprocessor.config.AttributionConfig;
import com.mercedes.streamprocessor.consumer.StreamConsumer;
import com.mercedes.streamprocessor.engine.JoinEngine;
import com.mercedes.streamprocessor.model.AttributedPageView;
import com.mercedes.streamprocessor.model.PageViewEvent;
import com.mercedes.streamprocessor.output.OutputSink;
import com.mercedes.streamprocessor.output.SqliteOutputSink;
import com.mercedes.streamprocessor.state.ClickStateStore;
import com.mercedes.streamprocessor.state.PageViewBufferStore;
import com.mercedes.streamprocessor.state.WatermarkTracker;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;

/**
 * Restart-safety tests. Tests 1 and 2 use @EmbeddedKafka (slow startup, ~10s);
 * test 3 is a pure unit test.
 *
 * @TestMethodOrder is required because the tests share a single JoinEngine bean,
 * so watermarks accumulate across methods. Each test uses a higher event_time
 * base than the previous one to avoid being dropped as too-late.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 2, topics = {"page_views", "ad_clicks"})
@Import(RestartTest.TestSinkConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
class RestartTest {

    private static Path tempDir;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) throws Exception {
        tempDir = Files.createTempDirectory("restart-test-");
        registry.add("output.database.path", () -> tempDir.resolve("test.db").toString());
        // Bridge embedded Kafka's randomly-assigned brokers to our property.
        registry.add("kafka.bootstrap-servers", () ->
                System.getProperty("spring.embedded.kafka.brokers"));
    }

    /**
     * Replaces the real sink with a wrapper that can throw on the Nth flush.
     * Avoids @SpyBean (which fails on Java 25 due to Mockito bytecode limits).
     */
    @TestConfiguration
    static class TestSinkConfig {
        @Bean
        @Primary
        FaultInjectableSink faultInjectableSink(SqliteOutputSink delegate) {
            return new FaultInjectableSink(delegate);
        }
    }

    static class FaultInjectableSink implements OutputSink {
        private final OutputSink delegate;
        private int flushCount = 0;
        private int failOnFlush = -1;
        private int faultsTriggered = 0;

        FaultInjectableSink(OutputSink delegate) { this.delegate = delegate; }

        synchronized void armFailOnFlush(int n) {
            this.flushCount = 0;
            this.faultsTriggered = 0;
            this.failOnFlush = n;
        }

        synchronized void disarm() { this.failOnFlush = -1; }

        synchronized int faultsTriggered() { return faultsTriggered; }

        @Override
        public void write(AttributedPageView record) { delegate.write(record); }

        @Override
        public synchronized void flush() {
            flushCount++;
            if (flushCount == failOnFlush) {
                faultsTriggered++;
                failOnFlush = -1; // one-shot
                throw new RuntimeException("Injected fault on flush #" + flushCount);
            }
            delegate.flush();
        }
    }

    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private KafkaListenerEndpointRegistry registry;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private FaultInjectableSink faultSink;

    @Value("${output.database.path}")
    private String dbPath;

    @BeforeEach
    void cleanState() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement s = c.createStatement()) {
            s.execute("DELETE FROM attributed_page_views");
        }
        faultSink.disarm();

        // Block until consumers finish initial partition assignment, otherwise
        // the first records produced can be missed.
        for (var container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 2);
        }
        Thread.sleep(500); // settle time after rebalance
    }

    /**
     * Process 5 events, stop and restart the listeners, process 5 more. Assert
     * the final SQLite has exactly 10 distinct rows (no duplicates, no missing).
     */
    @Test
    @Order(1)
    void restartFromCommittedOffsets() throws Exception {
        // eventTime base: minute 0 (this test runs first).
        for (int i = 1; i <= 5; i++) {
            sendPageView("pv_round1_" + i, "user_" + i, i);
        }
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(rowCount()).isEqualTo(5));

        // Stop and restart the listeners. This simulates a clean shutdown.
        registry.getListenerContainers().forEach(c -> c.stop());
        await().atMost(Duration.ofSeconds(5))
                .until(() -> registry.getListenerContainers().stream().noneMatch(c -> c.isRunning()));
        registry.getListenerContainers().forEach(c -> c.start());
        await().atMost(Duration.ofSeconds(10))
                .until(() -> registry.getListenerContainers().stream().allMatch(c -> c.isRunning()));
        for (var container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 2);
        }

        // Round 2: produce 5 more events.
        for (int i = 6; i <= 10; i++) {
            sendPageView("pv_round2_" + i, "user_" + i, i);
        }
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(rowCount()).isEqualTo(10));

        // No duplicates: page_view_id PK enforces uniqueness.
        assertThat(distinctPageViewCount()).isEqualTo(10);
    }

    // ---------------------------------------------------------------
    // Test 2 — fault before ack causes replay; sink dedup keeps output unique
    // ---------------------------------------------------------------
    @Test
    @Order(2)
    void noDataLossOnCrashBeforeAck() throws Exception {
        // Throw on the 3rd flush; one-shot.
        faultSink.armFailOnFlush(3);

        // eventTime base: minute 1000 (above test 1's max to clear watermark).
        for (int i = 1; i <= 5; i++) {
            sendPageView("pv_fault_" + i, "user_" + i, 1000 + i);
        }

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {
                    assertThat(faultSink.faultsTriggered()).isEqualTo(1);
                    assertThat(rowCount()).isEqualTo(5);
                });

        // Replay must not produce duplicate rows.
        assertThat(distinctPageViewCount()).isEqualTo(5);
    }

    // ---------------------------------------------------------------
    // Test 3 — write → flush → ack ordering (pure unit test, no Kafka)
    // ---------------------------------------------------------------
    @Test
    @Order(3)
    void ackOnlyAfterSinkFlush() throws Exception {
        OutputSink sink = Mockito.mock(OutputSink.class);
        Acknowledgment ack = Mockito.mock(Acknowledgment.class);
        ObjectMapper om = new ObjectMapper().registerModule(new JavaTimeModule());
        AttributionConfig config = new AttributionConfig(30);
        JoinEngine engine = new JoinEngine(
                new ClickStateStore(config),
                new PageViewBufferStore(config),
                new WatermarkTracker(2),
                sink,
                config);
        StreamConsumer consumer = new StreamConsumer(engine, om);

        PageViewEvent pv = PageViewEvent.builder()
                .eventId("pv_1")
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .url("/x")
                .build();
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("page_views", 0, 0L, "user_1", om.writeValueAsString(pv));

        consumer.consumePageView(record, ack);

        InOrder inOrder = Mockito.inOrder(sink, ack);
        inOrder.verify(sink).write(any(AttributedPageView.class));
        inOrder.verify(sink).flush();
        inOrder.verify(ack).acknowledge();
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private void sendPageView(String eventId, String userId, int minuteOffset) throws Exception {
        PageViewEvent pv = PageViewEvent.builder()
                .eventId(eventId)
                .userId(userId)
                .eventTime(Instant.parse("2024-01-01T12:00:00Z").plus(Duration.ofMinutes(minuteOffset)))
                .url("/p/" + eventId)
                .build();
        kafkaTemplate.send("page_views", userId, objectMapper.writeValueAsString(pv)).get();
    }

    private long rowCount() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM attributed_page_views")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private long distinctPageViewCount() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(DISTINCT page_view_id) FROM attributed_page_views")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
