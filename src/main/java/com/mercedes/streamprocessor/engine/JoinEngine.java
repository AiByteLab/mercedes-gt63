package com.mercedes.streamprocessor.engine;

import com.mercedes.streamprocessor.config.AttributionConfig;
import com.mercedes.streamprocessor.model.AdClickEvent;
import com.mercedes.streamprocessor.model.AttributedPageView;
import com.mercedes.streamprocessor.model.PageViewEvent;
import com.mercedes.streamprocessor.output.OutputSink;
import com.mercedes.streamprocessor.state.ClickStateStore;
import com.mercedes.streamprocessor.state.PageViewBufferStore;
import com.mercedes.streamprocessor.state.WatermarkTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Core join engine that performs windowed attribution joins between page views and ad clicks.
 *
 * Join semantics:
 * - For each page_view, find the most recent ad_click for the same user
 *   within 30 minutes before the page view (in event time)
 * - Handle out-of-order arrivals through watermark tracking
 *
 * Concurrency: per-user locking. Different users still run in parallel across consumer threads.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {

    private final ClickStateStore clickStore;
    private final PageViewBufferStore pageViewBuffer;
    private final WatermarkTracker watermarkTracker;
    private final OutputSink outputSink;
    private final AttributionConfig attributionConfig;

    private final ConcurrentHashMap<String, Object> userLocks = new ConcurrentHashMap<>();

    private Object lockFor(String userId) {
        return userLocks.computeIfAbsent(userId, k -> new Object());
    }

    /**
     * Process an ad click event.
     * Store the click and backpatch any already-emitted PVs that this click should now attribute.
     *
     * @param click the ad click event
     */
    public void processClick(AdClickEvent click) {
        log.debug("Processing click: {}", click.getClickId());
        synchronized (lockFor(click.getUserId())) {
            if (watermarkTracker.isTooLate(click.getPartition(), click.getEventTime())) {
                log.warn("Click is too late: {}", click.getClickId());
                return;
            }
            clickStore.addClick(click);

            // Backpatch: any PV in [click.event_time, click.event_time + window] for the same
            // user might now have a better attribution. Recompute and re-emit if changed.
            Collection<PageViewEvent> affected = pageViewBuffer.findAttributablePageViews(
                    click.getUserId(), click.getEventTime());
            if (!affected.isEmpty()) {
                for (PageViewEvent pv : affected) {
                    AdClickEvent best = clickStore.findAttributableClick(pv.getUserId(), pv.getEventTime());
                    outputSink.write(buildAttributed(pv, best));
                }
                outputSink.flush();
            }
        }
        watermarkTracker.updateWatermark(click.getPartition(), click.getEventTime());
    }

    /**
     * Process a page view event.
     * Emit immediately with the best currently-known click; buffer for possible backpatch.
     *
     * @param pv the page view event
     */
    public void processPageView(PageViewEvent pv) {
        log.debug("Processing page view: {}", pv.getEventId());
        synchronized (lockFor(pv.getUserId())) {
            if (watermarkTracker.isTooLate(pv.getPartition(), pv.getEventTime())) {
                log.warn("PageView is too late: {}", pv.getEventId());
                return;
            }
            // Buffer the PV before the write so a later same-user click can find it
            // for backpatching.
            pageViewBuffer.addPageView(pv);
            AdClickEvent click = clickStore.findAttributableClick(pv.getUserId(), pv.getEventTime());
            outputSink.write(buildAttributed(pv, click));
            outputSink.flush();
        }
        watermarkTracker.updateWatermark(pv.getPartition(), pv.getEventTime());
    }

    private AttributedPageView buildAttributed(PageViewEvent pv, AdClickEvent click) {
        return AttributedPageView.builder()
                .pageViewId(pv.getEventId())
                .userId(pv.getUserId())
                .eventTime(pv.getEventTime())
                .url(pv.getUrl())
                .attributedCampaignId(click == null ? null : click.getCampaignId())
                .attributedClickId(click == null ? null : click.getClickId())
                .build();
    }

    /**
     * Periodic state eviction. Both stores are bounded by the watermark:
     *   - PVs older than min_watermark are settled (no future click can change them).
     *   - Clicks older than min_watermark - window can no longer attribute any future PV.
     */
    @Scheduled(fixedRate = 30000)
    public void evictOldState() {
        Instant minWatermark = watermarkTracker.getMinWatermark();
        if (minWatermark.equals(Instant.MIN)) {
            log.debug("No watermarks yet, skipping eviction");
            return;
        }
        pageViewBuffer.evictOldPageViews(minWatermark);
        clickStore.evictOldClicks(minWatermark.minus(attributionConfig.getAttributionWindow()));
    }
}
