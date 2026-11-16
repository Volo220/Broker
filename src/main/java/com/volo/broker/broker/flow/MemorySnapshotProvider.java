package com.volo.broker.broker.flow;

import com.volo.broker.diagnostics.memory.MemoryMetricsSnapshot;

/**
 * Port providing memory telemetry snapshots to flow control and backpressure monitors.
 */
public interface MemorySnapshotProvider {

    /**
     * Obtains the current snapshot of memory metrics.
     *
     * @return non-null memory metrics snapshot
     */
    MemoryMetricsSnapshot currentSnapshot();
}
