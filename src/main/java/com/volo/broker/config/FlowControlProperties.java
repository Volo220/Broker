package com.volo.broker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "broker.flow-control")
public record FlowControlProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("0") long maxBlockMs,
        @DefaultValue Global global,
        @DefaultValue("64") int resumeBatch
) {
    public FlowControlProperties {
        if (maxBlockMs < 0) {
            maxBlockMs = 0L;
        }
        if (global == null) {
            global = new Global(0.40, 0.75, 0.80, 256L, 500L);
        }
        if (resumeBatch <= 0) {
            resumeBatch = 64;
        }
    }

    public record Global(
            @DefaultValue("0.40") double highRatio,
            @DefaultValue("0.75") double lowRatioFactor,
            @DefaultValue("0.80") double heapPressureThreshold,
            @DefaultValue("256") long minFreeOsMb,
            @DefaultValue("500") long checkIntervalMs
    ) {
        public Global {
            if (highRatio <= 0.0 || highRatio >= 1.0) highRatio = 0.40;
            if (lowRatioFactor <= 0.0 || lowRatioFactor >= 1.0) lowRatioFactor = 0.75;
            if (heapPressureThreshold <= 0.0 || heapPressureThreshold > 1.0) heapPressureThreshold = 0.80;
            if (minFreeOsMb < 0) minFreeOsMb = 256;
            if (checkIntervalMs <= 0) checkIntervalMs = 500;
        }

        public long minFreeOsBytes() {
            return minFreeOsMb * 1024L * 1024L;
        }
    }
}
