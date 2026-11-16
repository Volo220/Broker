package com.volo.broker.diagnostics.memory;

/**
 * Provides host operating system physical memory statistics.
 */
public interface HostMemoryProvider {

    /**
     * Reads current host physical memory availability and capacity.
     *
     * @return current host memory snapshot; never null
     */
    HostMemorySnapshot read();

    /**
     * Host physical memory measurements in bytes. Negative values indicate unavailable metrics.
     */
    record HostMemorySnapshot(long availableBytes, long totalBytes) {
        public static final HostMemorySnapshot UNAVAILABLE = new HostMemorySnapshot(-1L, -1L);
    }
}
