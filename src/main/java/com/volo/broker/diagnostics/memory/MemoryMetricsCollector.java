package com.volo.broker.diagnostics.memory;

import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.config.MemorySamplerProperties;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.management.*;
import java.time.Instant;
import java.util.List;

@Component
public class MemoryMetricsCollector {

    private final MemoryMXBean memoryMXBean;
    private final ThreadMXBean threadMXBean;
    private final RuntimeMXBean runtimeMXBean;
    private final List<GarbageCollectorMXBean> gcBeans;
    private final List<MemoryPoolMXBean> memoryPoolBeans;
    private final List<BufferPoolMXBean> bufferPoolBeans;
    private final GlobalMemoryGuard globalMemoryGuard;
    private final QueueManager queueManager;
    private final MemorySamplerProperties.MetricsConfig metricsConfig;

    @Autowired
    public MemoryMetricsCollector(
            MemorySamplerProperties properties,
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
        this.globalMemoryGuard = globalMemoryGuard;
        this.queueManager = queueManager;
    }

    public MemoryMetricsSnapshot collect() {
        Instant now = Instant.now();
        long uptimeSec = runtimeMXBean != null ? runtimeMXBean.getUptime() / 1000 : 0L;

        long heapUsed = -1L;
        long heapCommitted = -1L;
        long heapMax = -1L;
        if (memoryMXBean != null) {
            MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
            if (heapUsage != null) {
                heapUsed = heapUsage.getUsed();
                heapCommitted = heapUsage.getCommitted();
                heapMax = heapUsage.getMax();
            }
        }

        long oldGenUsed = -1L;
        long oldGenAfterGc = -1L;
        for (MemoryPoolMXBean pool : memoryPoolBeans) {
            if (isOldGenPool(pool)) {
                MemoryUsage usage = pool.getUsage();
                if (usage != null) {
                    oldGenUsed = usage.getUsed();
                }
                MemoryUsage collectionUsage = pool.getCollectionUsage();
                if (collectionUsage != null) {
                    oldGenAfterGc = collectionUsage.getUsed();
                }
                break;
            }
        }

        long directUsed = -1L;
        long directCapacity = -1L;
        if (metricsConfig.includeDirectMemory()) {
            for (BufferPoolMXBean pool : bufferPoolBeans) {
                if ("direct".equalsIgnoreCase(pool.getName())) {
                    directUsed = pool.getMemoryUsed();
                    directCapacity = pool.getTotalCapacity();
                    break;
                }
            }
        }

        long gcCount = -1L;
        long gcTimeMs = -1L;
        if (metricsConfig.includeGc()) {
            long totalCount = 0L;
            long totalTime = 0L;
            for (GarbageCollectorMXBean gc : gcBeans) {
                long c = gc.getCollectionCount();
                if (c > 0) totalCount += c;
                long t = gc.getCollectionTime();
                if (t > 0) totalTime += t;
            }
            gcCount = totalCount;
            gcTimeMs = totalTime;
        }

        int liveThreads = -1;
        if (metricsConfig.includeThreads() && threadMXBean != null) {
            liveThreads = threadMXBean.getThreadCount();
        }

        long brokerReadyBytes = -1L;
        int totalQueues = -1;
        if (metricsConfig.includeBrokerQueues()) {
            brokerReadyBytes = globalMemoryGuard != null ? globalMemoryGuard.totalReadyBytes() : 0L;
            totalQueues = queueManager != null ? queueManager.all().size() : 0;
        }

        return new MemoryMetricsSnapshot(
                now,
                uptimeSec,
                heapUsed,
                heapCommitted,
                heapMax,
                oldGenUsed,
                oldGenAfterGc,
                directUsed,
                directCapacity,
                gcCount,
                gcTimeMs,
                brokerReadyBytes,
                totalQueues,
                liveThreads
        );
    }

    private static boolean isOldGenPool(MemoryPoolMXBean pool) {
        if (pool == null) return false;
        String name = pool.getName();
        if (name == null) return false;
        String lower = name.toLowerCase();
        return lower.contains("old") || lower.contains("tenured") || lower.contains("zheap") || lower.contains("shenandoah");
    }
}

