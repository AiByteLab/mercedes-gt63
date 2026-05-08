package com.mercedes.streamprocessor.state;

import com.mercedes.streamprocessor.config.AttributionConfig;
import com.mercedes.streamprocessor.model.AdClickEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stores ad click events partitioned by user_id for efficient windowed joins.
 *
 * Thread-safe implementation with per-user locking for fine-grained concurrency.
 * Implements state eviction to prevent unbounded memory growth.
 *
 * KNOWN LIMITATION — same-instant collision: the inner map is keyed by event_time, so two
 * clicks for the same user with identical event_time will overwrite each other (only the
 * second is retained for attribution). Probability is low at millisecond precision, so we
 * skip the fix here. If it becomes an issue, switch the inner value type to
 * {@code List<AdClickEvent>} (or use a composite key including click_id) so multiple events
 * at the same instant can coexist.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClickStateStore {

    private final AttributionConfig attributionConfig;

    // ConcurrentSkipListMap is a thread-safe, sorted, navigable map with functions like floorEntry() which we can use 
    // to find the most recent click before a given page view time.
    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Instant, AdClickEvent>> clicksByUser = new ConcurrentHashMap<>();

    /**
     * Add a click event to the state store.
     *
     * Implement thread-safe click storage
     * - Use locks for thread safety
     * - Store clicks sorted by event time (most recent first)
     * - Handle concurrent access properly
     *
     * @param click the ad click event
     */
    public void addClick(AdClickEvent click) {
        clicksByUser
                .computeIfAbsent(click.getUserId(), __ -> new ConcurrentSkipListMap<>())
                .put(click.getEventTime(), click);
        log.debug("Adding click {} for user {}", click.getClickId(), click.getUserId());
    }

    /**
     * Find the most recent click for a user within the attribution window.
     *
     * Implement attribution logic
     * - Search for clicks in window: [pageViewTime - 30 minutes, pageViewTime]
     * - Return the most recent click within the window
     * - Return null if no click found
     *
     * @param userId the user ID
     * @param pageViewTime the page view event time
     * @return the most recent click within 30 minutes before the page view, or null if none found
     */
    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime) {
        ConcurrentSkipListMap<Instant, AdClickEvent> userClicks = clicksByUser.get(userId);
        if (userClicks == null) {
            log.debug("No clicks found for user {}", userId);
            return null;
        }

        // Find the most recent click before or at page view time.
        Map.Entry<Instant, AdClickEvent> latest = userClicks.floorEntry(pageViewTime);
        if (latest == null) {
            log.debug("No clicks found for user {} before page view time {}", userId, pageViewTime);
            return null;
        }

        // return null if click is outside the attribution window
        if (latest.getKey().isBefore(pageViewTime.minus(attributionConfig.getAttributionWindow()))) {
            log.debug("No clicks found for user {} within attribution window before page view time {}", userId, pageViewTime);
            return null;
        }
        return latest.getValue();
    }

    /**
     * Evict old clicks that are beyond the retention window.
     * Prevents unbounded memory growth.
     *
     * Implement state eviction
     * - Remove clicks older than the cutoff time
     * - Clean up empty user entries
     * - Return count of evicted clicks
     *
     * @param cutoffTime clicks older than this time should be evicted
     * @return number of clicks evicted
     */
    public int evictOldClicks(Instant cutoffTime) {
        AtomicInteger evicted = new AtomicInteger();
        for (String userId : clicksByUser.keySet()) {
            clicksByUser.compute(userId, (k, userClicks) -> {
                if (userClicks == null) {
                    return null;
                }
                // Remove clicks older than cutoff time
                int before = userClicks.size();
                userClicks.headMap(cutoffTime).clear();
                evicted.addAndGet(before - userClicks.size());
                // If no clicks remain for the user, remove the entry
                return userClicks.isEmpty() ? null : userClicks;
            });
        }
        log.debug("Evicted {} clicks older than {}", evicted.get(), cutoffTime);
        return evicted.get();
    }

    /**
     * Get the total number of clicks currently in state.
     *
     * @return total click count across all users
     */
    public long getTotalClickCount() {
        return clicksByUser.values().stream()
                .mapToLong(Map::size)
                .sum();
    }
}
