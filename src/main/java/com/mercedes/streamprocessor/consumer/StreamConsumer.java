package com.mercedes.streamprocessor.consumer;

import com.mercedes.streamprocessor.engine.JoinEngine;
import com.mercedes.streamprocessor.model.AdClickEvent;
import com.mercedes.streamprocessor.model.PageViewEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that processes page view and ad click events.
 *
 * Uses Spring Kafka's concurrent message listener containers for partition-aware processing.
 * Implements manual offset commit after successful processing for at-least-once delivery.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamConsumer {

    private final JoinEngine joinEngine;
    private final ObjectMapper objectMapper;

    /**
     * Consume ad click events from Kafka.
     *
     * Each partition is processed by a dedicated thread (configured via concurrency).
     * Offsets are committed manually after successful processing to ensure at-least-once delivery.
     *
     * Processing steps:
     * - Parse JSON to AdClickEvent
     * - Set partition and offset metadata
     * - Process through joinEngine
     * - Acknowledge offset on success
     * - Handle errors appropriately
     */
    @KafkaListener(
        topics = "${kafka.topics.ad-clicks:ad_clicks}",
        groupId = "${kafka.consumer.group-id:stream-processor-group}",
        containerFactory = "adClickListenerContainerFactory"
    )
    public void consumeAdClick(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.debug("Received ad click from partition {} at offset {}",
                record.partition(), record.offset());

            // Parse the JSON record value to AdClickEvent
            AdClickEvent click = objectMapper.readValue(record.value(), AdClickEvent.class);

            // Set partition and offset metadata on the event
            click.setPartition(record.partition());
            click.setOffset(record.offset());

            // Process the click through the join engine
            joinEngine.processClick(click);

            // Acknowledge the offset after successful processing
            acknowledgment.acknowledge();

            log.debug("Successfully processed ad click from partition {} offset {}",
                record.partition(), record.offset());

        } catch (Exception e) {
            log.error("Error processing ad click from partition {} offset {}: {}",
                record.partition(), record.offset(), record.value(), e);
            // Don't acknowledge - will be retried
            throw new RuntimeException("Failed to process ad click", e);
        }
    }

    /**
     * Consume page view events from Kafka.
     *
     * Each partition is processed by a dedicated thread (configured via concurrency).
     * Offsets are committed manually after successful processing to ensure at-least-once delivery.
     *
     * Processing steps:
     * - Parse JSON to PageViewEvent
     * - Set partition and offset metadata
     * - Process through joinEngine
     * - Acknowledge offset on success
     * - Handle errors appropriately
     */
    @KafkaListener(
        topics = "${kafka.topics.page-views:page_views}",
        groupId = "${kafka.consumer.group-id:stream-processor-group}",
        containerFactory = "pageViewListenerContainerFactory"
    )
    public void consumePageView(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.debug("Received page view from partition {} at offset {}",
                record.partition(), record.offset());

            // Parse the JSON record value to PageViewEvent
            PageViewEvent pageView = objectMapper.readValue(record.value(), PageViewEvent.class);

            // Set partition and offset metadata on the event
            pageView.setPartition(record.partition());
            pageView.setOffset(record.offset());

            // Process the page view through the join engine
            joinEngine.processPageView(pageView);

            // Acknowledge the offset after successful processing
            acknowledgment.acknowledge();

            log.debug("Successfully processed page view from partition {} offset {}",
                record.partition(), record.offset());

        } catch (Exception e) {
            log.error("Error processing page view from partition {} offset {}: {}",
                record.partition(), record.offset(), record.value(), e);
            // Don't acknowledge - will be retried
            throw new RuntimeException("Failed to process page view", e);
        }
    }
}
