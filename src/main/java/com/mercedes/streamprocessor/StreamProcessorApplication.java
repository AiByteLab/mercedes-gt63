package com.mercedes.streamprocessor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Main application for the Real-time Session Attribution Stream Processor.
 *
 * This application performs windowed joins between page view and ad click streams
 * to attribute page views to ad campaigns in real-time.
 */
@Slf4j
@SpringBootApplication
public class StreamProcessorApplication {

    public static void main(String[] args) {
        log.info("Starting Stream Processor Application...");
        SpringApplication.run(StreamProcessorApplication.class, args);
        log.info("Stream Processor Application started successfully");
    }
}
