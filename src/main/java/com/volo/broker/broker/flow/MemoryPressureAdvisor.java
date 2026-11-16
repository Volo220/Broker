package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import com.volo.broker.diagnostics.memory.MemoryMetricsSnapshot;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class MemoryPressureAdvisor {

    private static final long MIN_CEILING_BYTES = 10 * 1024 * 1024L; // 10 MB absolute minimum

    public MemoryAssessment assess(MemoryMetricsSnapshot snapshot, FlowControlProperties.Global config) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        FlowControlProperties.Global cfg = config != null
                ? config
                : new FlowControlProperties.Global(0.40, 0.75, 0.80, 256L, 500L);

        double allocatedHeapRatio = calculateHeapUsageRatio(snapshot);
        double liveHeapRatio = calculateLiveHeapUsageRatio(snapshot);
        double effectiveHeapRatio = liveHeapRatio >= 0.0 ? liveHeapRatio : allocatedHeapRatio;

        long freeOsMemoryBytes = snapshot.osFreeMemoryBytes();
        double freeOsMemoryRatio = calculateFreeOsMemoryRatio(snapshot);

        long ceilingBytes = calculateCeilingBytes(snapshot, freeOsMemoryBytes);
        long effectiveHighBytes = calculateDynamicHighBytes(ceilingBytes, cfg);
        long effectiveLowBytes = calculateDynamicLowBytes(effectiveHighBytes, cfg);

        PressureState pressure = evaluatePressure(liveHeapRatio, allocatedHeapRatio, freeOsMemoryBytes, cfg);

        return new MemoryAssessment(
                effectiveHeapRatio,
                freeOsMemoryBytes,
                freeOsMemoryRatio,
                effectiveHighBytes,
                effectiveLowBytes,
                pressure.underPressure(),
                pressure.reason()
        );
    }

    private static long calculateCeilingBytes(MemoryMetricsSnapshot snapshot, long freeOsMemoryBytes) {
        long maxHeap = snapshot.heapMaxBytes() > 0 ? snapshot.heapMaxBytes() : Runtime.getRuntime().maxMemory();
        long ceilingBytes = maxHeap;
        if (freeOsMemoryBytes > 0) {
            long currentHeap = snapshot.heapUsedBytes() > 0 ? snapshot.heapUsedBytes() : 0L;
            ceilingBytes = Math.min(maxHeap, freeOsMemoryBytes + currentHeap);
        }
        return Math.max(ceilingBytes, MIN_CEILING_BYTES);
    }

    private static long calculateDynamicHighBytes(long ceilingBytes, FlowControlProperties.Global config) {
        long high = (long) (ceilingBytes * config.highRatio());
        return Math.max(high, MIN_CEILING_BYTES);
    }

    private static long calculateDynamicLowBytes(long effectiveHighBytes, FlowControlProperties.Global config) {
        long low = (long) (effectiveHighBytes * config.lowRatioFactor());
        low = Math.max(low, MIN_CEILING_BYTES / 2);
        return low >= effectiveHighBytes ? effectiveHighBytes - 1 : low;
    }

    private static PressureState evaluatePressure(
            double liveHeapRatio,
            double allocatedHeapRatio,
            long freeOsMemoryBytes,
            FlowControlProperties.Global config
    ) {
        if (liveHeapRatio >= 0.0 && liveHeapRatio >= config.heapPressureThreshold()) {
            String reason = String.format("JVM_HEAP_PRESSURE: %.1f%% live heap after GC >= threshold %.1f%%",
                    liveHeapRatio * 100.0, config.heapPressureThreshold() * 100.0);
            return new PressureState(true, reason);
        }
        if (liveHeapRatio < 0.0 && allocatedHeapRatio >= config.heapPressureThreshold()) {
            String reason = String.format("JVM_HEAP_PRESSURE: %.1f%% used >= threshold %.1f%%",
                    allocatedHeapRatio * 100.0, config.heapPressureThreshold() * 100.0);
            return new PressureState(true, reason);
        }
        if (freeOsMemoryBytes >= 0 && freeOsMemoryBytes <= config.minFreeOsBytes()) {
            String reason = String.format("HOST_MEMORY_EXHAUSTION: free OS RAM %d MB <= min threshold %d MB",
                    freeOsMemoryBytes / (1024 * 1024), config.minFreeOsMb());
            return new PressureState(true, reason);
        }
        return new PressureState(false, "");
    }

    private static double calculateHeapUsageRatio(MemoryMetricsSnapshot snapshot) {
        if (snapshot.heapMaxBytes() > 0 && snapshot.heapUsedBytes() >= 0) {
            return (double) snapshot.heapUsedBytes() / snapshot.heapMaxBytes();
        }
        return 0.0;
    }

    private static double calculateLiveHeapUsageRatio(MemoryMetricsSnapshot snapshot) {
        if (snapshot.heapMaxBytes() > 0 && snapshot.oldGenAfterGcBytes() >= 0) {
            return (double) snapshot.oldGenAfterGcBytes() / snapshot.heapMaxBytes();
        }
        return -1.0;
    }

    private static double calculateFreeOsMemoryRatio(MemoryMetricsSnapshot snapshot) {
        if (snapshot.osTotalMemoryBytes() > 0 && snapshot.osFreeMemoryBytes() >= 0) {
            return (double) snapshot.osFreeMemoryBytes() / snapshot.osTotalMemoryBytes();
        }
        return 0.0;
    }

    private record PressureState(boolean underPressure, String reason) {}
}
