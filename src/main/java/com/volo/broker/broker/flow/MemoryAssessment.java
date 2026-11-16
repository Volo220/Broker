package com.volo.broker.broker.flow;

public record MemoryAssessment(
        double heapUsageRatio,
        long freeOsMemoryBytes,
        double freeOsMemoryRatio,
        long effectiveDynamicHighBytes,
        long effectiveDynamicLowBytes,
        boolean underMemoryPressure,
        String pressureReason
) {
    public double freeOsMemoryMb() {
        return Math.round((freeOsMemoryBytes / (1024.0 * 1024.0)) * 10.0) / 10.0;
    }

    public double effectiveDynamicHighMb() {
        return Math.round((effectiveDynamicHighBytes / (1024.0 * 1024.0)) * 10.0) / 10.0;
    }

    public double effectiveDynamicLowMb() {
        return Math.round((effectiveDynamicLowBytes / (1024.0 * 1024.0)) * 10.0) / 10.0;
    }
}
