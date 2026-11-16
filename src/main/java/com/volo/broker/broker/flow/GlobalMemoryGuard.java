package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class GlobalMemoryGuard {
    private final AtomicLong totalReadyBytes = new AtomicLong(0);
    private final AtomicLong effectiveHighBytes = new AtomicLong(0);
    private final AtomicLong effectiveLowBytes = new AtomicLong(0);
    private final AtomicBoolean externalMemoryPressure = new AtomicBoolean(false);
    private volatile String currentBlockReason = "";
    private final boolean enabled;
    private volatile FlowState globalState = FlowState.OPEN;

    @Autowired
    public GlobalMemoryGuard(FlowControlProperties properties) {
        if (properties != null && properties.enabled() && properties.global() != null) {
            this.enabled = true;
            long maxHeap = Runtime.getRuntime().maxMemory();
            long initialHigh = Math.max(10 * 1024 * 1024L, (long) (maxHeap * properties.global().highRatio()));
            long initialLow = Math.max(5 * 1024 * 1024L, (long) (initialHigh * properties.global().lowRatioFactor()));
            this.effectiveHighBytes.set(initialHigh);
            this.effectiveLowBytes.set(initialLow);
        } else {
            this.enabled = false;
            this.effectiveHighBytes.set(Long.MAX_VALUE);
            this.effectiveLowBytes.set(Long.MAX_VALUE / 2);
        }
    }

    public GlobalMemoryGuard(long initialHighBytes, long initialLowBytes) {
        this.enabled = true;
        this.effectiveHighBytes.set(initialHighBytes);
        this.effectiveLowBytes.set(initialLowBytes);
    }

    public FlowTransition recordBytesAdded(long bytes) {
        if (bytes <= 0 || !enabled) {
            return FlowTransition.NONE;
        }

        long current = totalReadyBytes.addAndGet(bytes);
        long high = effectiveHighBytes.get();
        if (globalState == FlowState.OPEN && (current >= high || externalMemoryPressure.get())) {
            synchronized (this) {
                if (globalState == FlowState.OPEN && (totalReadyBytes.get() >= effectiveHighBytes.get() || externalMemoryPressure.get())) {
                    globalState = FlowState.BLOCKED;
                    if (!externalMemoryPressure.get()) {
                        currentBlockReason = String.format("GLOBAL_HIGH_WATERMARK: %d MB >= %d MB",
                                totalReadyBytes.get() / (1024 * 1024), effectiveHighBytes.get() / (1024 * 1024));
                    }
                    return FlowTransition.TO_BLOCKED;
                }
            }
        }
        return FlowTransition.NONE;
    }

    public FlowTransition recordBytesRemoved(long bytes) {
        if (bytes <= 0 || !enabled) {
            return FlowTransition.NONE;
        }

        long current = totalReadyBytes.addAndGet(-bytes);
        if (current < 0) {
            totalReadyBytes.compareAndSet(current, 0);
            current = 0;
        }

        long low = effectiveLowBytes.get();
        if (globalState == FlowState.BLOCKED && current <= low && !externalMemoryPressure.get()) {
            synchronized (this) {
                if (globalState == FlowState.BLOCKED && totalReadyBytes.get() <= effectiveLowBytes.get() && !externalMemoryPressure.get()) {
                    globalState = FlowState.OPEN;
                    currentBlockReason = "";
                    return FlowTransition.TO_OPEN;
                }
            }
        }
        return FlowTransition.NONE;
    }

    public synchronized FlowTransition updateDynamicWatermarks(long newHighBytes, long newLowBytes) {
        if (!enabled || newHighBytes <= 0 || newLowBytes <= 0) {
            return FlowTransition.NONE;
        }

        effectiveHighBytes.set(newHighBytes);
        effectiveLowBytes.set(Math.min(newLowBytes, newHighBytes - 1));

        long current = totalReadyBytes.get();
        if (globalState == FlowState.OPEN && (current >= newHighBytes || externalMemoryPressure.get())) {
            globalState = FlowState.BLOCKED;
            if (!externalMemoryPressure.get()) {
                currentBlockReason = String.format("DYNAMIC_WATERMARK_EXCEEDED: %d MB >= %d MB",
                        current / (1024 * 1024), newHighBytes / (1024 * 1024));
            }
            return FlowTransition.TO_BLOCKED;
        } else if (globalState == FlowState.BLOCKED && current <= newLowBytes && !externalMemoryPressure.get()) {
            globalState = FlowState.OPEN;
            currentBlockReason = "";
            return FlowTransition.TO_OPEN;
        }

        return FlowTransition.NONE;
    }

    public synchronized FlowTransition updateExternalPressure(boolean underPressure, String reason) {
        if (!enabled) {
            return FlowTransition.NONE;
        }

        boolean wasPressure = externalMemoryPressure.getAndSet(underPressure);
        if (underPressure) {
            currentBlockReason = (reason != null && !reason.isBlank()) ? reason : "EXTERNAL_MEMORY_PRESSURE";
            if (globalState == FlowState.OPEN) {
                globalState = FlowState.BLOCKED;
                return FlowTransition.TO_BLOCKED;
            }
        } else if (wasPressure) {
            currentBlockReason = "";
            long current = totalReadyBytes.get();
            if (globalState == FlowState.BLOCKED && current <= effectiveLowBytes.get()) {
                globalState = FlowState.OPEN;
                return FlowTransition.TO_OPEN;
            }
        }

        return FlowTransition.NONE;
    }

    public boolean isBlocked() {
        return enabled && globalState == FlowState.BLOCKED;
    }

    public FlowState globalState() {
        return globalState;
    }

    public long totalReadyBytes() {
        return totalReadyBytes.get();
    }

    public long effectiveHighBytes() {
        return effectiveHighBytes.get();
    }

    public long effectiveLowBytes() {
        return effectiveLowBytes.get();
    }

    public boolean isExternalMemoryPressure() {
        return externalMemoryPressure.get();
    }

    public String currentBlockReason() {
        return currentBlockReason != null ? currentBlockReason : "";
    }
}
