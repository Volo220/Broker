package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Component
public class QueueFlowController implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(QueueFlowController.class);

    private final ConcurrentHashMap<String, QueueFlowEntry> queues = new ConcurrentHashMap<>();
    private final Set<FlowGate> globalWaiters = ConcurrentHashMap.newKeySet();
    private final GlobalMemoryGuard globalMemoryGuard;
    private final int resumeBatch;
    private final long defaultMaxBlockMs;
    private final ExecutorService resumeExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final FlowTimeoutScheduler timeoutScheduler;
    private final FlowControlMetrics metrics = new FlowControlMetrics();

    public QueueFlowController(GlobalMemoryGuard globalMemoryGuard, FlowControlProperties properties) {
        this.globalMemoryGuard = Objects.requireNonNull(globalMemoryGuard, "globalMemoryGuard must not be null");
        this.resumeBatch = properties != null && properties.resumeBatch() > 0 ? properties.resumeBatch() : 64;
        this.defaultMaxBlockMs = properties != null ? properties.maxBlockMs() : 0;
        this.timeoutScheduler = new FlowTimeoutScheduler(this::checkOverdueGates, 200L);
    }

    public boolean isAnyBlocked(Collection<String> queuePaths) {
        if (globalMemoryGuard.isBlocked()) {
            return true;
        }
        if (queuePaths == null || queuePaths.isEmpty()) {
            return false;
        }
        for (String queuePath : queuePaths) {
            QueueFlowEntry entry = queues.get(queuePath);
            if (entry != null && entry.state == FlowState.BLOCKED) {
                return true;
            }
        }
        return false;
    }

    public boolean isQueueBlocked(String queuePath) {
        if (globalMemoryGuard.isBlocked()) {
            return true;
        }
        QueueFlowEntry entry = queues.get(queuePath);
        return entry != null && entry.state == FlowState.BLOCKED;
    }

    public void park(FlowGate gate, Collection<String> blockedQueuePaths) {
        Objects.requireNonNull(gate, "gate must not be null");
        if (blockedQueuePaths != null && !blockedQueuePaths.isEmpty()) {
            for (String queuePath : blockedQueuePaths) {
                queues.computeIfAbsent(queuePath, k -> new QueueFlowEntry()).waiters.add(gate);
            }
        } else {
            globalWaiters.add(gate);
        }

        // Double-check to prevent lost wakeup
        if (!isAnyBlocked(blockedQueuePaths)) {
            unregister(gate);
            gate.resume();
        }
    }

    public void unregister(FlowGate gate) {
        if (gate == null) return;
        globalWaiters.remove(gate);
        for (QueueFlowEntry entry : queues.values()) {
            entry.waiters.remove(gate);
        }
    }

    public void handleTransition(String queuePath, FlowTransition transition) {
        handleTransition(queuePath, transition, defaultMaxBlockMs);
    }

    public void handleTransition(String queuePath, FlowTransition transition, long maxBlockMs) {
        if (transition == null || transition == FlowTransition.NONE) {
            return;
        }

        QueueFlowEntry entry = queues.computeIfAbsent(queuePath, k -> new QueueFlowEntry());
        if (maxBlockMs > 0) {
            entry.maxBlockMs = maxBlockMs;
        }

        if (transition == FlowTransition.TO_BLOCKED) {
            entry.state = FlowState.BLOCKED;
            metrics.recordBlocked();
            log.debug("Queue '{}' transitioned to BLOCKED (maxBlockMs={})", queuePath, entry.maxBlockMs);
        } else if (transition == FlowTransition.TO_OPEN) {
            entry.state = FlowState.OPEN;
            metrics.recordOpen();
            log.debug("Queue '{}' transitioned to OPEN. Resuming waiters...", queuePath);
            wakeWaiters(entry.waiters);
        }
    }

    public void handleGlobalTransition(FlowTransition transition) {
        if (transition == FlowTransition.TO_OPEN) {
            metrics.recordOpen();
            log.debug("GlobalMemoryGuard transitioned to OPEN. Resuming all eligible waiters...");
            wakeEligibleWaiters();
        } else if (transition == FlowTransition.TO_BLOCKED) {
            metrics.recordBlocked();
            log.debug("GlobalMemoryGuard transitioned to BLOCKED: {}", globalMemoryGuard.currentBlockReason());
        }
    }

    public void wakeEligibleWaiters() {
        if (isGlobalBlocked()) {
            return;
        }

        wakeWaiters(globalWaiters);

        for (QueueFlowEntry entry : queues.values()) {
            if (entry.state == FlowState.OPEN && !entry.waiters.isEmpty()) {
                wakeWaiters(entry.waiters);
            }
        }
    }

    public boolean isGlobalBlocked() {
        return globalMemoryGuard.isBlocked();
    }

    public String globalBlockReason() {
        return globalMemoryGuard.currentBlockReason();
    }

    public GlobalMemoryGuard globalMemoryGuard() {
        return globalMemoryGuard;
    }

    public void onQueueDeleted(String queuePath) {
        QueueFlowEntry removed = queues.remove(queuePath);
        if (removed != null) {
            wakeWaiters(removed.waiters);
        }
    }

    public void recordOverdueRejection() {
        metrics.recordOverdueRejection();
    }

    public void checkOverdueGates() {
        long now = System.currentTimeMillis();

        if (!isGlobalBlocked()) {
            wakeEligibleWaiters();
        }

        for (QueueFlowEntry entry : queues.values()) {
            long effectiveTimeout = entry.maxBlockMs > 0 ? entry.maxBlockMs : defaultMaxBlockMs;
            if (entry.state == FlowState.BLOCKED || isGlobalBlocked()) {
                checkOverdueWaiters(entry.waiters, effectiveTimeout, now);
            }
        }
        if (isGlobalBlocked()) {
            checkOverdueWaiters(globalWaiters, defaultMaxBlockMs, now);
        }
    }

    private static void checkOverdueWaiters(Set<FlowGate> waiters, long timeoutMs, long now) {
        if (timeoutMs <= 0 || waiters.isEmpty()) {
            return;
        }
        for (FlowGate gate : waiters) {
            long pausedAt = gate.pausedAtTimestamp();
            if (pausedAt > 0 && (now - pausedAt) >= timeoutMs) {
                gate.notifyOverdue();
            }
        }
    }

    private void wakeWaiters(Set<FlowGate> waitersSet) {
        if (waitersSet == null || waitersSet.isEmpty()) {
            return;
        }

        List<FlowGate> batch = new ArrayList<>(resumeBatch);
        Iterator<FlowGate> it = waitersSet.iterator();
        while (it.hasNext() && batch.size() < resumeBatch) {
            FlowGate gate = it.next();
            it.remove();
            batch.add(gate);
        }

        if (batch.isEmpty()) {
            return;
        }

        resumeExecutor.submit(() -> {
            for (FlowGate gate : batch) {
                try {
                    if (isGateBlockedOnAnyOtherQueue(gate)) {
                        waitersSet.add(gate);
                    } else {
                        gate.resume();
                    }
                } catch (Throwable t) {
                    log.warn("Error while resuming flow gate: {}", t.getMessage());
                }
            }
        });
    }

    private boolean isGateBlockedOnAnyOtherQueue(FlowGate gate) {
        if (globalMemoryGuard.isBlocked()) {
            return true;
        }
        for (QueueFlowEntry entry : queues.values()) {
            if (entry.state == FlowState.BLOCKED && entry.waiters.contains(gate)) {
                return true;
            }
        }
        return false;
    }

    public long totalTransitionsToBlocked() {
        return metrics.totalTransitionsToBlocked();
    }

    public long totalTransitionsToOpen() {
        return metrics.totalTransitionsToOpen();
    }

    public long overdueTimeoutRejections() {
        return metrics.overdueTimeoutRejections();
    }

    @Override
    @PreDestroy
    public void close() {
        timeoutScheduler.close();
        resumeExecutor.shutdown();
        metrics.logShutdownSummary();
    }
}
