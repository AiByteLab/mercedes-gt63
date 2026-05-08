package com.mercedes.streamprocessor.engine;

import com.mercedes.streamprocessor.config.AttributionConfig;
import com.mercedes.streamprocessor.model.AdClickEvent;
import com.mercedes.streamprocessor.model.AttributedPageView;
import com.mercedes.streamprocessor.model.PageViewEvent;
import com.mercedes.streamprocessor.output.OutputSink;
import com.mercedes.streamprocessor.state.ClickStateStore;
import com.mercedes.streamprocessor.state.PageViewBufferStore;
import com.mercedes.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the JoinEngine under emit-and-update semantics. One test per README scenario:
 *   1. clickBeforePageView: normal in-order attribution.
 *   2. multipleClicksPicksLatest: multiple clicks in window, attribute to the latest.
 *   3. clickOutside30MinWindowNotAttributed: click older than 30 minutes does not attribute.
 *   4. outOfOrderClickArrival: click arrives after the PV; the row is backpatched.
 *   5. veryLateEventDropped: click beyond allowedLateness is dropped.
 *   6. pageViewWithNoClick: PV with no matching click emits null attribution.
 *
 * Each test asserts on the last emit for the user, since under emit-and-update an
 * out-of-order click can produce a follow-up emit and we care about the final state.
 */
class JoinEngineTest {

    private static final Instant T = Instant.parse("2024-01-01T12:00:00Z");

    // Far-future event_time used by some tests to advance the watermark — kept from
    // the earlier emit-once design. Under emit-and-update it is a no-op for most
    // tests; veryLateEventDropped is the one case that genuinely relies on it.
    private static final Instant DRAIN_TRIGGER = T.plus(Duration.ofHours(1));

    private static final AttributionConfig ATTRIBUTION_CONFIG = new AttributionConfig(30);

    private CapturingSink sink;
    private ClickStateStore clickStore;
    private PageViewBufferStore pageViewBuffer;
    private WatermarkTracker watermarkTracker;
    private JoinEngine engine;

    @BeforeEach
    void setUp() {
        sink = new CapturingSink();
        clickStore = new ClickStateStore(ATTRIBUTION_CONFIG);
        pageViewBuffer = new PageViewBufferStore(ATTRIBUTION_CONFIG);
        watermarkTracker = new WatermarkTracker(2); // 2-minute lateness
        engine = new JoinEngine(clickStore, pageViewBuffer, watermarkTracker, sink, ATTRIBUTION_CONFIG);
    }

    // ---------------------------------------------------------------
    // Test 1 — normal in-order attribution (click before page view)
    // ---------------------------------------------------------------
    @Test
    void clickBeforePageView() {
        engine.processClick(click("user_1", T.plus(Duration.ofMinutes(5)), "campaign_A", "click_1"));
        engine.processPageView(pageView("user_1", T.plus(Duration.ofMinutes(10)), "pv_1"));

        engine.processClick(click("trigger_user", DRAIN_TRIGGER, "x", "x"));

        AttributedPageView emitted = lastEmittedFor("user_1");
        assertThat(emitted.getAttributedCampaignId()).isEqualTo("campaign_A");
        assertThat(emitted.getAttributedClickId()).isEqualTo("click_1");
    }

    // ---------------------------------------------------------------
    // Test 2 — multiple clicks in window, attribute to the latest
    // ---------------------------------------------------------------
    @Test
    void multipleClicksPicksLatest() {
        engine.processClick(click("user_3", T.plus(Duration.ofMinutes(5)), "campaign_C", "click_3a"));
        engine.processClick(click("user_3", T.plus(Duration.ofMinutes(10)), "campaign_D", "click_3b"));
        engine.processPageView(pageView("user_3", T.plus(Duration.ofMinutes(15)), "pv_3"));

        engine.processClick(click("trigger_user", DRAIN_TRIGGER, "x", "x"));

        AttributedPageView emitted = lastEmittedFor("user_3");
        assertThat(emitted.getAttributedCampaignId()).isEqualTo("campaign_D");
        assertThat(emitted.getAttributedClickId()).isEqualTo("click_3b");
    }

    // ---------------------------------------------------------------
    // Test 3 — click older than the 30-min attribution window, no attribution
    // ---------------------------------------------------------------
    @Test
    void clickOutside30MinWindowNotAttributed() {
        engine.processClick(click("user_4", T, "campaign_E", "click_4"));
        engine.processPageView(pageView("user_4", T.plus(Duration.ofMinutes(35)), "pv_4"));

        engine.processClick(click("trigger_user", DRAIN_TRIGGER, "x", "x"));

        AttributedPageView emitted = lastEmittedFor("user_4");
        assertThat(emitted.getAttributedCampaignId()).isNull();
        assertThat(emitted.getAttributedClickId()).isNull();
    }

    // ---------------------------------------------------------------
    // Test 4 — out-of-order click within lateness, row is backpatched
    // ---------------------------------------------------------------
    @Test
    void outOfOrderClickArrival() {
        // PV arrives before its click. With emit-and-update, the PV is emitted immediately
        // with null attribution; when the click arrives, the engine backpatches the row.
        // Lateness bumped to 10 min so the late click isn't dropped as too-late.
        watermarkTracker = new WatermarkTracker(10);
        engine = new JoinEngine(clickStore, pageViewBuffer, watermarkTracker, sink, ATTRIBUTION_CONFIG);

        engine.processPageView(pageView("user_2", T.plus(Duration.ofMinutes(10)), "pv_2"));
        engine.processClick(click("user_2", T.plus(Duration.ofMinutes(5)), "campaign_B", "click_2"));

        AttributedPageView emitted = lastEmittedFor("user_2");
        assertThat(emitted.getAttributedCampaignId()).isEqualTo("campaign_B");
        assertThat(emitted.getAttributedClickId()).isEqualTo("click_2");
    }

    // ---------------------------------------------------------------
    // Test 5 — click beyond allowedLateness is dropped silently
    // ---------------------------------------------------------------
    @Test
    void veryLateEventDropped() {
        // Push watermark past T+7 so cutoff (watermark - 2min) > T+5.
        engine.processClick(click("trigger_user", T.plus(Duration.ofMinutes(8)), "x", "x"));

        // T+5 < T+8 - 2min = T+6 → too late, should be dropped silently.
        engine.processClick(click("user_5", T.plus(Duration.ofMinutes(5)), "campaign_5", "click_5"));

        AdClickEvent stored = clickStore.findAttributableClick(
                "user_5", T.plus(Duration.ofMinutes(10)));
        assertThat(stored).isNull();
    }

    // ---------------------------------------------------------------
    // Test 6 — page view with no click emits null attribution
    // ---------------------------------------------------------------
    @Test
    void pageViewWithNoClick() {
        engine.processPageView(pageView("user_6", T.plus(Duration.ofMinutes(10)), "pv_6"));

        engine.processClick(click("trigger_user", DRAIN_TRIGGER, "x", "x"));

        AttributedPageView emitted = lastEmittedFor("user_6");
        assertThat(emitted.getAttributedCampaignId()).isNull();
        assertThat(emitted.getAttributedClickId()).isNull();
    }

    /**
     * Returns the most recent emit for the given user. Under emit-and-update, an out-of-order
     * click can produce a follow-up emit; the test cares about the final attribution state.
     */
    private AttributedPageView lastEmittedFor(String userId) {
        List<AttributedPageView> matches = sink.writes.stream()
                .filter(r -> userId.equals(r.getUserId()))
                .toList();
        assertThat(matches).isNotEmpty();
        return matches.get(matches.size() - 1);
    }

    private static AdClickEvent click(String userId, Instant eventTime, String campaignId, String clickId) {
        return AdClickEvent.builder()
                .userId(userId)
                .eventTime(eventTime)
                .campaignId(campaignId)
                .clickId(clickId)
                .build();
    }

    private static PageViewEvent pageView(String userId, Instant eventTime, String eventId) {
        return PageViewEvent.builder()
                .userId(userId)
                .eventTime(eventTime)
                .url("/page")
                .eventId(eventId)
                .build();
    }

    /** In-memory sink that captures emitted records for assertions. */
    static class CapturingSink implements OutputSink {
        final List<AttributedPageView> writes = new ArrayList<>();

        @Override
        public void write(AttributedPageView record) {
            writes.add(record);
        }

        @Override
        public void flush() {
            // no-op
        }
    }
}
