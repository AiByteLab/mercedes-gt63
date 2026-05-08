package com.mercedes.streamprocessor.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the four core properties of WatermarkTracker:
 *   1. uninitialized partitions report Instant.MIN.
 *   2. an observed event_time stores the watermark as event_time - allowedLateness.
 *   3. an event with event_time before the watermark is reported as too-late.
 *   4. the watermark only ever moves forward, even if events arrive out of order.
 */
class WatermarkTrackerTest {

    private static final Instant T = Instant.parse("2026-05-04T12:00:00Z");
    private static final Duration LATENESS = Duration.ofMinutes(2);

    private WatermarkTracker tracker;

    @BeforeEach
    void setUp() {
        // 2-minute lateness; the test math below assumes this value, independent
        // of the production default in application.yml.
        tracker = new WatermarkTracker(LATENESS.toMinutesPart());
    }

    // ---------------------------------------------------------------
    // Test 1 — uninitialized partitions return Instant.MIN
    // ---------------------------------------------------------------
    @Test
    void initialWatermarkIsMin() {
        assertThat(tracker.getWatermark(0)).isEqualTo(Instant.MIN);
    }

    // ---------------------------------------------------------------
    // Test 2 — updateWatermark stores event_time - allowedLateness
    // ---------------------------------------------------------------
    @Test
    void watermarkUpdatesWithEvents() {
        tracker.updateWatermark(0, T);

        // Watermark = observed event_time - allowedLateness.
        assertThat(tracker.getWatermark(0)).isEqualTo(T.minus(LATENESS));
    }

    // ---------------------------------------------------------------
    // Test 3 — isTooLate returns true for events before the watermark
    // ---------------------------------------------------------------
    @Test
    void lateEventDetection() {
        Instant observed = T.plus(Duration.ofMinutes(10));
        tracker.updateWatermark(0, observed);
        // Watermark is now observed - 2min = T+8min.

        // T < T+8min → too late.
        assertThat(tracker.isTooLate(0, T)).isTrue();
        // T+9min is after the watermark (T+8min) → not late.
        assertThat(tracker.isTooLate(0, observed.minus(Duration.ofMinutes(1)))).isFalse();
    }

    // ---------------------------------------------------------------
    // Test 4 — out-of-order updates do not roll the watermark backwards
    // ---------------------------------------------------------------
    @Test
    void watermarkMonotonicallyIncreases() {
        Instant later = T.plus(Duration.ofMinutes(10));
        Instant earlier = T.plus(Duration.ofMinutes(5));

        tracker.updateWatermark(0, later);
        tracker.updateWatermark(0, earlier);

        // Watermark tracks the highest observation, minus lateness.
        assertThat(tracker.getWatermark(0)).isEqualTo(later.minus(LATENESS));
    }
}
