package com.mercedes.streamprocessor.output;

import com.mercedes.streamprocessor.model.AttributedPageView;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * SQLite-based output sink. Writes are idempotent on page_view_id.
 */
@Slf4j
@Component
public class SqliteOutputSink implements OutputSink {

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS attributed_page_views (" +
                    "page_view_id TEXT PRIMARY KEY, " +
                    "user_id TEXT NOT NULL, " +
                    "event_time TEXT NOT NULL, " +
                    "url TEXT, " +
                    "attributed_campaign_id TEXT, " +
                    "attributed_click_id TEXT" +
                    ")";

    private static final String INSERT_SQL =
            "INSERT OR REPLACE INTO attributed_page_views " +
                    "(page_view_id, user_id, event_time, url, attributed_campaign_id, attributed_click_id) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";

    private final Connection conn;

    public SqliteOutputSink(@Value("${output.database.path:./output/attributed_page_views.db}") String dbPath) {
        try {
            var parent = Paths.get(dbPath).toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);

            this.conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            this.conn.setAutoCommit(false);
            initSchema();
            log.info("Initialized SQLite output sink at {}", dbPath);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize SQLite output sink at " + dbPath, e);
        }
    }

    private void initSchema() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
        }
        conn.commit();
    }

    @Override
    public synchronized void write(AttributedPageView record) {
        try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            ps.setString(1, record.getPageViewId());
            ps.setString(2, record.getUserId());
            ps.setString(3, record.getEventTime() == null ? null : record.getEventTime().toString());
            ps.setString(4, record.getUrl());
            ps.setString(5, record.getAttributedCampaignId());
            ps.setString(6, record.getAttributedClickId());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to write " + record.getPageViewId(), e);
        }
    }

    @Override
    public synchronized void flush() {
        try {
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException rollbackEx) {
                log.warn("Rollback failed after flush failure", rollbackEx);
            }
            throw new IllegalStateException("Failed to flush", e);
        }
    }

    @PreDestroy
    public synchronized void close() {
        try {
            if (!conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            log.warn("Error closing SQLite connection", e);
        }
    }
}
