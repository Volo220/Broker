package com.volo.broker.broker.flow;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically invokes a timeout-checking task to detect parked {@link FlowGate} instances
 * exceeding maximum block durations.
 */
class FlowTimeoutScheduler implements AutoCloseable {

    private final ScheduledExecutorService scheduler;

    FlowTimeoutScheduler(Runnable checkTask, long periodMs) {
        Objects.requireNonNull(checkTask, "checkTask must not be null");
        long period = periodMs > 0 ? periodMs : 200L;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "flow-control-timeout-checker");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(checkTask, period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
