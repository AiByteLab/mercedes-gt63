package com.mercedes.streamprocessor.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents an ad click event from the stream.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AdClickEvent {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("event_time")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss", timezone = "UTC")
    private Instant eventTime;

    @JsonProperty("campaign_id")
    private String campaignId;

    @JsonProperty("click_id")
    private String clickId;

    // Metadata fields for processing
    private transient int partition;
    private transient long offset;
}
