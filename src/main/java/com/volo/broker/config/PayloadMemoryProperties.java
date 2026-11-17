package com.volo.broker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration properties for payload memory management.
 */
@ConfigurationProperties(prefix = "broker.memory.payload")
public record PayloadMemoryProperties(
        @DefaultValue("HEAP") PayloadMode mode,
        @DefaultValue("256") int inlineThresholdBytes,
        @DefaultValue ScratchPool scratchPool
) {
    public enum PayloadMode {
        HEAP,
        POOLED_DIRECT
    }

    public record ScratchPool(
            @DefaultValue("16384") int bufferSizeBytes,
            @DefaultValue("256") int maxPooled
    ) {
        public ScratchPool {
            if (bufferSizeBytes <= 0) bufferSizeBytes = 16384;
            if (maxPooled <= 0) maxPooled = 256;
        }
    }

    public PayloadMemoryProperties {
        if (mode == null) mode = PayloadMode.HEAP;
        if (inlineThresholdBytes < 0) inlineThresholdBytes = 256;
        if (scratchPool == null) scratchPool = new ScratchPool(16384, 256);
    }
}
