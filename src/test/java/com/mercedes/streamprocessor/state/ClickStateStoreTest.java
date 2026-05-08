package com.mercedes.streamprocessor.state;

import com.mercedes.streamprocessor.config.AttributionConfig;
import com.mercedes.streamprocessor.model.AdClickEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the four core properties of ClickStateStore:
 *   1. addAndRetrieveClick: a stored click is found by findAttributableClick.
 *   2. evictionRemovesOldClicks: clicks older than the cutoff are dropped.
 *   3. latestClickInWindow: when multiple clicks fall in the attribution window,
 *      the most recent one is returned.
 *   4. threadSafetyUnderConcurrentAccess: concurrent addClick from many threads
 *      does not lose writes or corrupt state.
 */
class ClickStateStoreTest {

    private static final Instant T = Instant.parse("2024-01-01T12:00:00Z");

    private ClickStateStore store;

    @BeforeEach
    void setUp() {
        store = new ClickStateStore(new AttributionConfig(30));
    }

    // ---------------------------------------------------------------
    // Test 1 — add a click, retrieve it within the attribution window
    // ---------------------------------------------------------------
    @Test
    void addAndRetrieveClick() {
        AdClickEvent click = click("user_1", T.plus(Duration.ofMinutes(5)), "campaign_A", "click_1");

        store.addClick(click);

        AdClickEvent found = store.findAttributableClick("user_1", T.plus(Duration.ofMinutes(10)));
        assertThat(found).isEqualTo(click);
    }

    // ---------------------------------------------------------------
    // Test 2 — evictOldClicks removes clicks older than the cutoff
    // ---------------------------------------------------------------
    @Test
    void evictionRemovesOldClicks() {
        AdClickEvent oldClick = click("user_1", T, "campaign_A", "old_click");
        AdClickEvent recentClick = click("user_1", T.plus(Duration.ofMinutes(20)), "campaign_B", "recent_click");
        store.addClick(oldClick);
        store.addClick(recentClick);

        int evicted = store.evictOldClicks(T.plus(Duration.ofMinutes(10)));

        assertThat(evicted).isEqualTo(1);
        assertThat(store.getTotalClickCount()).isEqualTo(1);
        assertThat(store.findAttributableClick("user_1", T.plus(Duration.ofMinutes(25))))
                .isEqualTo(recentClick);
    }

    // ---------------------------------------------------------------
    // Test 3 — return the most recent click when several fall in window
    // ---------------------------------------------------------------
    @Test
    void latestClickInWindow() {
        AdClickEvent earlier = click("user_1", T.plus(Duration.ofMinutes(5)), "campaign_C", "click_3a");
        AdClickEvent later = click("user_1", T.plus(Duration.ofMinutes(10)), "campaign_D", "click_3b");
        store.addClick(earlier);
        store.addClick(later);

        AdClickEvent found = store.findAttributableClick("user_1", T.plus(Duration.ofMinutes(15)));

        assertThat(found).isEqualTo(later);
    }

    // ---------------------------------------------------------------
    // Test 4 — concurrent addClick from many threads, no lost writes
    // ---------------------------------------------------------------
    @Test
    void threadSafetyUnderConcurrentAccess() throws InterruptedException {
        int threads = 10;
        int clicksPerThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < clicksPerThread; i++) {
                        store.addClick(click(
                                "user_" + threadId,
                                T.plus(Duration.ofSeconds(i)),
                                "campaign_X",
                                "click_" + threadId + "_" + i));
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        boolean finishedInTime = done.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(finishedInTime).isTrue();
        assertThat(failures.get()).isZero();
        assertThat(store.getTotalClickCount()).isEqualTo((long) threads * clicksPerThread);
    }

    private static AdClickEvent click(String userId, Instant eventTime, String campaignId, String clickId) {
        return AdClickEvent.builder()
                .userId(userId)
                .eventTime(eventTime)
                .campaignId(campaignId)
                .clickId(clickId)
                .build();
    }
}
