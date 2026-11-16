package com.volo.broker.diagnostics.memory;

import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.config.MemorySamplerProperties;
import com.volo.broker.diagnostics.memory.HostMemoryProvider.HostMemorySnapshot;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MemoryMetricsCollectorTest {

    @Test
    void collect_shouldReturnValidSnapshotFromStandardJvm() {
        MemorySamplerProperties props = new MemorySamplerProperties(
                true,
                1000,
                new MemorySamplerProperties.CsvConfig(true, "logs/test.csv", false),
                new MemorySamplerProperties.ConsoleConfig(true, 5),
                new MemorySamplerProperties.MetricsConfig(true, true, true, true)
        );

        GlobalMemoryGuard guard = new GlobalMemoryGuard(1000L, 500L);
        guard.recordBytesAdded(1024 * 1024);

        QueueManager queueManager = mock(QueueManager.class);
        when(queueManager.all()).thenReturn(java.util.List.of());

        HostMemoryProvider hostMemoryProvider = () -> new HostMemorySnapshot(8_000_000_000L, 16_000_000_000L);
        MemoryMetricsCollector collector = new MemoryMetricsCollector(props, hostMemoryProvider, guard, queueManager);

        MemoryMetricsSnapshot snapshot = collector.collect();

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.timestamp()).isNotNull();
        assertThat(snapshot.uptimeSec()).isGreaterThanOrEqualTo(0);
        assertThat(snapshot.heapUsedBytes()).isPositive();
        assertThat(snapshot.heapCommittedBytes()).isPositive();
        assertThat(snapshot.heapMaxBytes()).isPositive();
        assertThat(snapshot.liveThreads()).isPositive();
        assertThat(snapshot.brokerReadyBytes()).isEqualTo(1024 * 1024);
        assertThat(snapshot.totalQueues()).isZero();
        assertThat(snapshot.osFreeMemoryBytes()).isEqualTo(8_000_000_000L);
        assertThat(snapshot.osTotalMemoryBytes()).isEqualTo(16_000_000_000L);
    }

    @Test
    void collect_withDisabledMetrics_shouldSetNegativeValues() {
        MemorySamplerProperties props = new MemorySamplerProperties(
                true,
                1000,
                null,
                null,
                new MemorySamplerProperties.MetricsConfig(false, false, false, false)
        );

        MemoryMetricsCollector collector = new MemoryMetricsCollector(props, null, null, null);

        MemoryMetricsSnapshot snapshot = collector.collect();

        assertThat(snapshot.gcCount()).isEqualTo(-1L);
        assertThat(snapshot.gcTimeMs()).isEqualTo(-1L);
        assertThat(snapshot.directMemoryUsedBytes()).isEqualTo(-1L);
        assertThat(snapshot.liveThreads()).isEqualTo(-1);
        assertThat(snapshot.brokerReadyBytes()).isEqualTo(-1L);
        assertThat(snapshot.totalQueues()).isEqualTo(-1);
        assertThat(snapshot.osFreeMemoryBytes()).isEqualTo(-1L);
        assertThat(snapshot.osTotalMemoryBytes()).isEqualTo(-1L);
    }

    @Test
    void collect_whenHostMemoryProviderFailsOrReturnsNull_setsUnavailableOsMemory() {
        MemorySamplerProperties props = new MemorySamplerProperties(
                true,
                1000,
                null,
                null,
                new MemorySamplerProperties.MetricsConfig(true, true, true, true)
        );

        MemoryMetricsCollector failingCollector = new MemoryMetricsCollector(
                props,
                () -> {
                    throw new RuntimeException("provider failure");
                },
                null,
                null
        );
        MemoryMetricsSnapshot failedSnapshot = failingCollector.collect();
        assertThat(failedSnapshot.osFreeMemoryBytes()).isEqualTo(-1L);
        assertThat(failedSnapshot.osTotalMemoryBytes()).isEqualTo(-1L);

        MemoryMetricsCollector nullResultCollector = new MemoryMetricsCollector(props, () -> null, null, null);
        MemoryMetricsSnapshot nullSnapshot = nullResultCollector.collect();
        assertThat(nullSnapshot.osFreeMemoryBytes()).isEqualTo(-1L);
        assertThat(nullSnapshot.osTotalMemoryBytes()).isEqualTo(-1L);
    }
}
