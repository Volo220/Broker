package com.volo.broker.diagnostics.memory;

import java.time.Instant;

public record MemoryMetricsSnapshot(
        Instant timestamp,
        long uptimeSec,
        long heapUsedBytes,
        long heapCommittedBytes,
        long heapMaxBytes,
        long oldGenUsedBytes,
        long oldGenAfterGcBytes,
        long directMemoryUsedBytes,
        long directMemoryCapacityBytes,
        long gcCount,
        long gcTimeMs,
        long brokerReadyBytes,
        int totalQueues,
        int liveThreads
) {
    public double heapUsedMb() {
        return toMb(heapUsedBytes);
    }

    public double heapCommittedMb() {
        return toMb(heapCommittedBytes);
    }

    public double heapMaxMb() {
        return toMb(heapMaxBytes);
    }

    public double oldGenUsedMb() {
        return toMb(oldGenUsedBytes);
    }

    public double oldGenAfterGcMb() {
        return toMb(oldGenAfterGcBytes);
    }

    public double directMemoryUsedMb() {
        return toMb(directMemoryUsedBytes);
    }

    public double directMemoryCapacityMb() {
        return toMb(directMemoryCapacityBytes);
    }

    public double brokerReadyMb() {
        return toMb(brokerReadyBytes);
    }

    private static double toMb(long bytes) {
        if (bytes < 0) {
            return -1.0;
        }
        return Math.round((bytes / (1024.0 * 1024.0)) * 10.0) / 10.0;
    }
}

