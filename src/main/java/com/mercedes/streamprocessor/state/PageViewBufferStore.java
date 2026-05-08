package com.mercedes.streamprocessor.state;

import com.mercedes.streamprocessor.config.AttributionConfig;
import com.mercedes.streamprocessor.model.PageViewEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-user buffer of recently emitted page views. The engine uses this to find which PVs
 * a late-arriving click should backpatch.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PageViewBufferStore {

    private final AttributionConfig attributionConfig;

    private final ConcurrentHashMap<String, ConcurrentSkipListMap<Instant, PageViewEvent>>
            pageViewsByUser = new ConcurrentHashMap<>();

    public void addPageView(PageViewEvent pv) {
        pageViewsByUser
                .computeIfAbsent(pv.getUserId(), __ -> new ConcurrentSkipListMap<>())
                .put(pv.getEventTime(), pv);
        log.debug("Buffered page view {} for user {}", pv.getEventId(), pv.getUserId());
    }

    /**
     * Return PVs for the given user that this click could attribute:
     * pv.event_time ∈ [click.event_time, click.event_time + attribution_window].
     */
    public Collection<PageViewEvent> findAttributablePageViews(String userId, Instant clickEventTime) {
        ConcurrentSkipListMap<Instant, PageViewEvent> userPvs = pageViewsByUser.get(userId);
        if (userPvs == null) return Collections.emptyList();

        Instant windowEnd = clickEventTime.plus(attributionConfig.getAttributionWindow());
        // subMap is exclusive at the end by default; use the inclusive overload.
        Map<Instant, PageViewEvent> inWindow = userPvs.subMap(clickEventTime, true, windowEnd, true);
        return List.copyOf(inWindow.values());
    }

    /**
     * Drop PVs older than the cutoff (= the partition watermark). Such PVs are settled —
     * no future click can affect their attribution.
     */
    public int evictOldPageViews(Instant cutoff) {
        AtomicInteger evicted = new AtomicInteger();
        for (String userId : pageViewsByUser.keySet()) {
            pageViewsByUser.compute(userId, (k, userPvs) -> {
                if (userPvs == null) return null;
                int before = userPvs.size();
                userPvs.headMap(cutoff).clear();
                evicted.addAndGet(before - userPvs.size());
                return userPvs.isEmpty() ? null : userPvs;
            });
        }
        log.debug("Evicted {} page views older than {}", evicted.get(), cutoff);
        return evicted.get();
    }

    public long getTotalPageViewCount() {
        return pageViewsByUser.values().stream().mapToLong(Map::size).sum();
    }
}
