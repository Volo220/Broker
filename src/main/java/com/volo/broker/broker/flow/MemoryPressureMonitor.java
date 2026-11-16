package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import com.volo.broker.diagnostics.memory.MemoryMetricsSnapshot;
import jakarta.annotation.PreDestroy;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class MemoryPressureMonitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MemoryPressureMonitor.class);
    public static final long GC_REQUEST_COOLDOWN_MS = 5000L;

    private final FlowControlProperties properties;
    private final GlobalMemoryGuard globalMemoryGuard;
    private final QueueFlowController flowController;
    private final MemorySnapshotProvider snapshotProvider;
    private final MemoryPressureAdvisor advisor;
    private final ScheduledExecutorService scheduler;
    private volatile boolean running;
    private volatile long lastGcRequestTimeMs = 0L;

    @Autowired
    public MemoryPressureMonitor(
            FlowControlProperties properties,
            GlobalMemoryGuard globalMemoryGuard,
            @Nullable QueueFlowController flowController,
            MemorySnapshotProvider snapshotProvider,
            MemoryPressureAdvisor advisor
    ) {
        this.properties = properties;
        this.globalMemoryGuard = Objects.requireNonNull(globalMemoryGuard, "globalMemoryGuard must not be null");
        this.flowController = flowController;
        this.snapshotProvider = Objects.requireNonNull(snapshotProvider, "snapshotProvider must not be null");
        this.advisor = Objects.requireNonNull(advisor, "advisor must not be null");

        if (properties != null && properties.enabled() && properties.global() != null) {
            long intervalMs = properties.global().checkIntervalMs();
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "memory-pressure-monitor");
                t.setDaemon(true);
                return t;
            });
            this.running = true;
            this.scheduler.scheduleWithFixedDelay(this::checkPressure, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
            log.info("MemoryPressureMonitor started (checkIntervalMs={}, minFreeOsMb={}, heapThreshold={}%)",
                    intervalMs,
                    properties.global().minFreeOsMb(),
                    properties.global().heapPressureThreshold() * 100.0);
        } else {
            this.scheduler = null;
            this.running = false;
        }
    }

    public void checkPressure() {
        if (!running) {
            return;
        }

        try {
            MemoryMetricsSnapshot snapshot = obtainFreshSnapshot();
            MemoryAssessment assessment = advisor.assess(snapshot, properties.global());
            applyAssessment(assessment);
        } catch (Throwable t) {
            log.warn("Error during memory pressure check: {}", t.getMessage());
        }
    }

    private MemoryMetricsSnapshot obtainFreshSnapshot() {
        MemoryMetricsSnapshot snapshot = snapshotProvider.currentSnapshot();
        if (shouldRequestGc(snapshot)) {
            triggerRateLimitedGc(snapshot);
            snapshot = snapshotProvider.currentSnapshot();
        }
        return snapshot;
    }

    private void applyAssessment(MemoryAssessment assessment) {
        FlowTransition watermarkTransition = globalMemoryGuard.updateDynamicWatermarks(
                assessment.effectiveDynamicHighBytes(),
                assessment.effectiveDynamicLowBytes()
        );
        if (watermarkTransition != FlowTransition.NONE && flowController != null) {
            flowController.handleGlobalTransition(watermarkTransition);
        }

        FlowTransition pressureTransition = globalMemoryGuard.updateExternalPressure(
                assessment.underMemoryPressure(),
                assessment.pressureReason()
        );
        if (pressureTransition != FlowTransition.NONE && flowController != null) {
            flowController.handleGlobalTransition(pressureTransition);
        }

        if (!globalMemoryGuard.isBlocked() && flowController != null) {
            flowController.wakeEligibleWaiters();
        }
    }

    boolean shouldRequestGc(MemoryMetricsSnapshot snapshot) {
        if (snapshot == null || snapshot.heapMaxBytes() <= 0 || snapshot.heapUsedBytes() <= 0) {
            return false;
        }

        double heapUsedRatio = (double) snapshot.heapUsedBytes() / snapshot.heapMaxBytes();
        double threshold = (properties != null && properties.global() != null)
                ? properties.global().heapPressureThreshold()
                : 0.80;

        if (heapUsedRatio < threshold) {
            return false;
        }

        long now = System.currentTimeMillis();
        if (now - lastGcRequestTimeMs < GC_REQUEST_COOLDOWN_MS) {
            return false;
        }

        if (snapshot.oldGenAfterGcBytes() > 0) {
            double liveRatio = (double) snapshot.oldGenAfterGcBytes() / snapshot.heapMaxBytes();
            if (liveRatio >= threshold) {
                return false;
            }
        }

        return true;
    }

    void triggerRateLimitedGc(MemoryMetricsSnapshot snapshot) {
        lastGcRequestTimeMs = System.currentTimeMillis();
        log.info("Requesting System.gc() to reclaim uncollected heap garbage (allocated: {}/{} MB, oldGenLive: {} MB, backlog: {} MB)...",
                snapshot.heapUsedMb(), snapshot.heapMaxMb(), snapshot.oldGenAfterGcMb(), snapshot.brokerReadyMb());
        System.gc();
    }

    void setLastGcRequestTimeMs(long timeMs) {
        this.lastGcRequestTimeMs = timeMs;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    @PreDestroy
    public synchronized void close() {
        if (!running) {
            return;
        }
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            log.info("MemoryPressureMonitor stopped.");
        }
    }
}
