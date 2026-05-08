package com.mercedes.streamprocessor.output;

import com.mercedes.streamprocessor.model.AttributedPageView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the two properties we rely on from SqliteOutputSink:
 *   1. write + flush persists a row.
 *   2. a duplicate write for the same page_view_id produces only one row
 *      (INSERT OR REPLACE on the primary key, i.e., replay-safe).
 *
 * Null-attribution rows are covered end-to-end by JoinEngineTest::pageViewWithNoClick.
 */
class SqliteOutputSinkTest {

    @TempDir
    Path tempDir;

    private SqliteOutputSink sink;
    private String dbPath;

    @BeforeEach
    void setUp() {
        dbPath = tempDir.resolve("test.db").toString();
        sink = new SqliteOutputSink(dbPath);
    }

    // ---------------------------------------------------------------
    // Test 1 — write + flush persists a row readable via JDBC
    // ---------------------------------------------------------------
    @Test
    void writePersistsRecord() throws Exception {
        AttributedPageView record = sampleRecord("pv_1", "campaign_A", "click_1");

        sink.write(record);
        sink.flush();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT page_view_id, user_id, attributed_campaign_id, attributed_click_id " +
                             "FROM attributed_page_views")) {

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("page_view_id")).isEqualTo("pv_1");
            assertThat(rs.getString("user_id")).isEqualTo("user_1");
            assertThat(rs.getString("attributed_campaign_id")).isEqualTo("campaign_A");
            assertThat(rs.getString("attributed_click_id")).isEqualTo("click_1");
            assertThat(rs.next()).isFalse();
        }
    }

    // ---------------------------------------------------------------
    // Test 2 — duplicate write is idempotent (INSERT OR REPLACE on PK)
    // ---------------------------------------------------------------
    @Test
    void idempotentDuplicateWrite() throws Exception {
        AttributedPageView record = sampleRecord("pv_1", "campaign_A", "click_1");

        sink.write(record);
        sink.write(record);
        sink.flush();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM attributed_page_views WHERE page_view_id = 'pv_1'")) {
            rs.next();
            assertThat(rs.getLong(1)).isEqualTo(1);
        }
    }

    private static AttributedPageView sampleRecord(String pageViewId, String campaignId, String clickId) {
        return AttributedPageView.builder()
                .pageViewId(pageViewId)
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .url("/home")
                .attributedCampaignId(campaignId)
                .attributedClickId(clickId)
                .build();
    }
}
