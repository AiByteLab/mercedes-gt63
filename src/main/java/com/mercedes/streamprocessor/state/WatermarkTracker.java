package com.mercedes.streamprocessor.state;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks watermarks per partition to handle out-of-order events.
 *
 * Watermark = max(observed event_time) - allowedLateness. Represents the point in event-time
 * up to which we trust all events have been seen. Events with event_time < watermark are late
 * and dropped.
 */
@Slf4j
@Component
public class WatermarkTracker {

    private final Duration allowedLateness;
    private final ConcurrentHashMap<Integer, Instant> watermarks = new ConcurrentHashMap<>();

    public WatermarkTracker(@Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes) {
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        log.info("Initialized WatermarkTracker with allowed lateness: {} minutes", allowedLatenessMinutes);
    }

    /**
     * Advance the watermark for a partition based on an observed event time.
     * Stored value is (eventTime - allowedLateness). Watermark advances monotonically.
     *
     * @param partition the partition ID
     * @param eventTime the observed event timestamp
     */
    public void updateWatermark(int partition, Instant eventTime) {
        Instant watermark = eventTime.minus(allowedLateness);
        watermarks.merge(partition, watermark, (current, incoming) -> incoming.isAfter(current) ? incoming : current);
        log.debug("Advanced watermark for partition {} to {} (from event {})", partition, watermark, eventTime);
    }

    /**
     * Get the current watermark for a partition.
     *
     * @param partition the partition ID
     * @return the current watermark, or Instant.MIN if not yet initialized
     */
    public Instant getWatermark(int partition) {
        return watermarks.getOrDefault(partition, Instant.MIN);
    }

    /**
     * Check if an event is too late (its event_time is before the partition's watermark).
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     * @return true if the event should be dropped
     */
    public boolean isTooLate(int partition, Instant eventTime) {
        Instant watermark = watermarks.get(partition);
        if (watermark == null) {
            return false;
        }
        return eventTime.isBefore(watermark);
    }

    /**
     * Get the allowed lateness duration.
     */
    public Duration getAllowedLateness() {
        return allowedLateness;
    }

    /**
     * Minimum watermark across all known partitions, or Instant.MIN if none.
     * Join engine uses this as the safe-emit / safe-evict horizon.
     */
    public Instant getMinWatermark() {
        return watermarks.values().stream()
                .min(Instant::compareTo)
                .orElse(Instant.MIN);
    }
}
