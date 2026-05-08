package com.mercedes.streamprocessor.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents the output of the join operation - a page view attributed to an ad click.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AttributedPageView {

    @JsonProperty("page_view_id")
    private String pageViewId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("event_time")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss", timezone = "UTC")
    private Instant eventTime;

    @JsonProperty("url")
    private String url;

    @JsonProperty("attributed_campaign_id")
    private String attributedCampaignId;

    @JsonProperty("attributed_click_id")
    private String attributedClickId;
}
