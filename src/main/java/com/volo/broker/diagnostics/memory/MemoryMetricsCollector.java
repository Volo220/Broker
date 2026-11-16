package com.volo.broker.diagnostics.memory;

import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.flow.MemorySnapshotProvider;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.config.MemorySamplerProperties;
import com.volo.broker.diagnostics.memory.HostMemoryProvider.HostMemorySnapshot;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.management.*;
import java.time.Instant;
import java.util.List;

@Component
public class MemoryMetricsCollector implements MemorySnapshotProvider {

    private final MemoryMXBean memoryMXBean;
    private final ThreadMXBean threadMXBean;
    private final RuntimeMXBean runtimeMXBean;
    private final List<GarbageCollectorMXBean> gcBeans;
    private final List<MemoryPoolMXBean> memoryPoolBeans;
    private final List<BufferPoolMXBean> bufferPoolBeans;
    private final @Nullable HostMemoryProvider hostMemoryProvider;
    private final GlobalMemoryGuard globalMemoryGuard;
    private final QueueManager queueManager;
    private final MemorySamplerProperties.MetricsConfig metricsConfig;

    @Autowired
    public MemoryMetricsCollector(
            MemorySamplerProperties properties,
            @Nullable HostMemoryProvider hostMemoryProvider,
            @Nullable GlobalMemoryGuard globalMemoryGuard,
            @Nullable QueueManager queueManager
    ) {
        this(
                properties != null && properties.metrics() != null
                        ? properties.metrics()
                        : new MemorySamplerProperties.MetricsConfig(true, true, true, true),
                ManagementFactory.getMemoryMXBean(),
                ManagementFactory.getThreadMXBean(),
                ManagementFactory.getRuntimeMXBean(),
                ManagementFactory.getGarbageCollectorMXBeans(),
                ManagementFactory.getMemoryPoolMXBeans(),
                ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class),
                hostMemoryProvider,
                globalMemoryGuard,
                queueManager
        );
    }

    public MemoryMetricsCollector(
            MemorySamplerProperties.MetricsConfig metricsConfig,
            MemoryMXBean memoryMXBean,
            ThreadMXBean threadMXBean,
            RuntimeMXBean runtimeMXBean,
            List<GarbageCollectorMXBean> gcBeans,
            List<MemoryPoolMXBean> memoryPoolBeans,
            List<BufferPoolMXBean> bufferPoolBeans,
            @Nullable HostMemoryProvider hostMemoryProvider,
            @Nullable GlobalMemoryGuard globalMemoryGuard,
            @Nullable QueueManager queueManager
    ) {
        this.metricsConfig = metricsConfig != null
                ? metricsConfig
                : new MemorySamplerProperties.MetricsConfig(true, true, true, true);
        this.memoryMXBean = memoryMXBean;
        this.threadMXBean = threadMXBean;
        this.runtimeMXBean = runtimeMXBean;
        this.gcBeans = gcBeans != null ? gcBeans : List.of();
        this.memoryPoolBeans = memoryPoolBeans != null ? memoryPoolBeans : List.of();
        this.bufferPoolBeans = bufferPoolBeans != null ? bufferPoolBeans : List.of();
        this.hostMemoryProvider = hostMemoryProvider;
        this.globalMemoryGuard = globalMemoryGuard;
        this.queueManager = queueManager;
    }

    @Override
    public MemoryMetricsSnapshot currentSnapshot() {
        return collect();
    }

    public MemoryMetricsSnapshot collect() {
        Instant now = Instant.now();
        long uptimeSec = runtimeMXBean != null ? runtimeMXBean.getUptime() / 1000 : 0L;

        HeapMetrics heap = readHeapMetrics();
        OldGenMetrics oldGen = readOldGenMetrics();
        DirectMetrics direct = readDirectMemoryMetrics();
        GcMetrics gc = readGcMetrics();
        int liveThreads = readLiveThreads();
        BrokerMetrics broker = readBrokerMetrics();
        HostMemorySnapshot hostMemory = readHostMemory();

        return new MemoryMetricsSnapshot(
                now,
                uptimeSec,
                heap.used(),
                heap.committed(),
                heap.max(),
                oldGen.used(),
                oldGen.afterGc(),
                direct.used(),
                direct.capacity(),
                gc.count(),
                gc.timeMs(),
                broker.readyBytes(),
                broker.totalQueues(),
                liveThreads,
                hostMemory.availableBytes(),
                hostMemory.totalBytes()
        );
    }

    private BrokerMetrics readBrokerMetrics() {
        if (!metricsConfig.includeBrokerQueues()) {
            return new BrokerMetrics(-1L, -1);
        }
        long readyBytes = globalMemoryGuard != null ? globalMemoryGuard.totalReadyBytes() : 0L;
        int queues = queueManager != null ? queueManager.all().size() : 0;
        return new BrokerMetrics(readyBytes, queues);
    }

    private HeapMetrics readHeapMetrics() {
        if (memoryMXBean == null) {
            return new HeapMetrics(-1L, -1L, -1L);
        }
        MemoryUsage usage = memoryMXBean.getHeapMemoryUsage();
        if (usage == null) {
            return new HeapMetrics(-1L, -1L, -1L);
        }
        return new HeapMetrics(usage.getUsed(), usage.getCommitted(), usage.getMax());
    }

    private OldGenMetrics readOldGenMetrics() {
        for (MemoryPoolMXBean pool : memoryPoolBeans) {
            if (isOldGenPool(pool)) {
                MemoryUsage usage = pool.getUsage();
                MemoryUsage collUsage = pool.getCollectionUsage();
                long used = usage != null ? usage.getUsed() : -1L;
                long afterGc = collUsage != null ? collUsage.getUsed() : -1L;
                return new OldGenMetrics(used, afterGc);
            }
        }
        return new OldGenMetrics(-1L, -1L);
    }

    private DirectMetrics readDirectMemoryMetrics() {
        if (!metricsConfig.includeDirectMemory()) {
            return new DirectMetrics(-1L, -1L);
        }
        for (BufferPoolMXBean pool : bufferPoolBeans) {
            if ("direct".equalsIgnoreCase(pool.getName())) {
                return new DirectMetrics(pool.getMemoryUsed(), pool.getTotalCapacity());
            }
        }
        return new DirectMetrics(-1L, -1L);
    }

    private GcMetrics readGcMetrics() {
        if (!metricsConfig.includeGc()) {
            return new GcMetrics(-1L, -1L);
        }
        long totalCount = 0L;
        long totalTime = 0L;
        for (GarbageCollectorMXBean gc : gcBeans) {
            long c = gc.getCollectionCount();
            if (c > 0) totalCount += c;
            long t = gc.getCollectionTime();
            if (t > 0) totalTime += t;
        }
        return new GcMetrics(totalCount, totalTime);
    }

    private int readLiveThreads() {
        if (metricsConfig.includeThreads() && threadMXBean != null) {
            return threadMXBean.getThreadCount();
        }
        return -1;
    }

    private record HeapMetrics(long used, long committed, long max) {}
    private record OldGenMetrics(long used, long afterGc) {}
    private record DirectMetrics(long used, long capacity) {}
    private record GcMetrics(long count, long timeMs) {}
    private record BrokerMetrics(long readyBytes, int totalQueues) {}

    private HostMemorySnapshot readHostMemory() {
        if (hostMemoryProvider == null) {
            return HostMemorySnapshot.UNAVAILABLE;
        }
        try {
            HostMemorySnapshot snapshot = hostMemoryProvider.read();
            return snapshot != null ? snapshot : HostMemorySnapshot.UNAVAILABLE;
        } catch (Throwable ignored) {
            return HostMemorySnapshot.UNAVAILABLE;
        }
    }

    private static boolean isOldGenPool(MemoryPoolMXBean pool) {
        if (pool == null) return false;
        String name = pool.getName();
        if (name == null) return false;
        String lower = name.toLowerCase();
        return lower.contains("old") || lower.contains("tenured") || lower.contains("zheap") || lower.contains("shenandoah");
    }
}
