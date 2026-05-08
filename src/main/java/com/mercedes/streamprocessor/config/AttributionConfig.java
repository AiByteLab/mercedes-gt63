package com.mercedes.streamprocessor.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Getter
public class AttributionConfig {

    private final Duration attributionWindow;

    public AttributionConfig(@Value("${attribution.window-minutes:30}") int windowMinutes) {
        this.attributionWindow = Duration.ofMinutes(windowMinutes);
    }
}
