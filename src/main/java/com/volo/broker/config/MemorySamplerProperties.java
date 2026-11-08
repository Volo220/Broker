package com.volo.broker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "broker.memory-sampler")
public record MemorySamplerProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("1000") long sampleIntervalMs,
        @DefaultValue CsvConfig csv,
        @DefaultValue ConsoleConfig console,
        @DefaultValue MetricsConfig metrics
) {
    public MemorySamplerProperties {
        if (sampleIntervalMs <= 0) {
            sampleIntervalMs = 1000L;
        }
        if (csv == null) {
            csv = new CsvConfig(true, "logs/memory-metrics.csv", false);
        }
        if (console == null) {
            console = new ConsoleConfig(true, 5);
        }
        if (metrics == null) {
            metrics = new MetricsConfig(true, true, true, true);
        }
    }

    public record CsvConfig(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("logs/memory-metrics.csv") String filePath,
            @DefaultValue("false") boolean append
    ) {
        public CsvConfig {
            if (filePath == null || filePath.isBlank()) {
                filePath = "logs/memory-metrics.csv";
            }
        }
    }

    public record ConsoleConfig(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("5") int intervalSeconds
    ) {
        public ConsoleConfig {
            if (intervalSeconds <= 0) {
                intervalSeconds = 5;
            }
        }
    }

    public record MetricsConfig(
            @DefaultValue("true") boolean includeGc,
            @DefaultValue("true") boolean includeDirectMemory,
            @DefaultValue("true") boolean includeThreads,
            @DefaultValue("true") boolean includeBrokerQueues
    ) {}
}

