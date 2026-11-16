package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import com.volo.broker.diagnostics.memory.MemoryMetricsSnapshot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MemoryPressureMonitorTest {

    @Test
    @DisplayName("checkPressure updates dynamic watermarks and triggers transition when memory exceeds threshold")
    void checkPressure_triggersWatermarkTransition() {
        long mb = 1024 * 1024L;
        FlowControlProperties.Global global = new FlowControlProperties.Global(
                0.40,
                0.75,
                0.80,
                256L,
                1000L
        );
        FlowControlProperties props = new FlowControlProperties(true, 0L, global, 64);
        GlobalMemoryGuard guard = new GlobalMemoryGuard(1000L * mb, 500L * mb);
        QueueFlowController flowController = mock(QueueFlowController.class);
        MemorySnapshotProvider snapshotProvider = mock(MemorySnapshotProvider.class);
        MemoryPressureAdvisor advisor = mock(MemoryPressureAdvisor.class);

        // Put some bytes in the guard (400 MB)
        guard.recordBytesAdded(400 * mb);

        MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                Instant.now(), 10L, 100 * mb, 200 * mb, 1000 * mb,
                50 * mb, 40 * mb, 10 * mb, 20 * mb, 1L, 10L, 400 * mb, 1, 5,
                2000 * mb, 4000 * mb
        );
        when(snapshotProvider.currentSnapshot()).thenReturn(snapshot);

        // Advisor returns lowered watermarks: High = 300 MB, Low = 200 MB
        MemoryAssessment assessment = new MemoryAssessment(
                0.10, 2000 * mb, 0.50, 300 * mb, 200 * mb, false, ""
        );
        when(advisor.assess(any(), any())).thenReturn(assessment);

        try (MemoryPressureMonitor monitor = new MemoryPressureMonitor(props, guard, flowController, snapshotProvider, advisor)) {
            monitor.checkPressure();

            assertThat(guard.isBlocked()).isTrue();
            assertThat(guard.effectiveHighBytes()).isEqualTo(300 * mb);
            assertThat(guard.effectiveLowBytes()).isEqualTo(200 * mb);
            verify(flowController, times(1)).handleGlobalTransition(FlowTransition.TO_BLOCKED);
        }
    }

    @Test
    @DisplayName("checkPressure triggers transition when under external memory pressure")
    void checkPressure_triggersExternalPressure() {
        long mb = 1024 * 1024L;
        FlowControlProperties.Global global = new FlowControlProperties.Global(
                0.40,
                0.75,
                0.80,
                256L,
                1000L
        );
        FlowControlProperties props = new FlowControlProperties(true, 0L, global, 64);
        GlobalMemoryGuard guard = new GlobalMemoryGuard(1000L * mb, 500L * mb);
        QueueFlowController flowController = mock(QueueFlowController.class);
        MemorySnapshotProvider snapshotProvider = mock(MemorySnapshotProvider.class);
        MemoryPressureAdvisor advisor = mock(MemoryPressureAdvisor.class);

        MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                Instant.now(), 10L, 900 * mb, 950 * mb, 1000 * mb,
                50 * mb, 40 * mb, 10 * mb, 20 * mb, 1L, 10L, 0L, 1, 5,
                2000 * mb, 4000 * mb
        );
        when(snapshotProvider.currentSnapshot()).thenReturn(snapshot);

        MemoryAssessment assessment = new MemoryAssessment(
                0.90, 2000 * mb, 0.50, 800 * mb, 600 * mb, true, "JVM_HEAP_PRESSURE: 90.0% used >= 80.0%"
        );
        when(advisor.assess(any(), any())).thenReturn(assessment);

        try (MemoryPressureMonitor monitor = new MemoryPressureMonitor(props, guard, flowController, snapshotProvider, advisor)) {
            monitor.checkPressure();

            assertThat(guard.isBlocked()).isTrue();
            assertThat(guard.isExternalMemoryPressure()).isTrue();
            assertThat(guard.currentBlockReason()).contains("JVM_HEAP_PRESSURE");
            verify(flowController, times(1)).handleGlobalTransition(FlowTransition.TO_BLOCKED);

            // Now pressure relieves
            MemoryAssessment relievedAssessment = new MemoryAssessment(
                0.20, 2000 * mb, 0.50, 800 * mb, 600 * mb, false, ""
            );
            when(advisor.assess(any(), any())).thenReturn(relievedAssessment);

            monitor.checkPressure();

            assertThat(guard.isBlocked()).isFalse();
            assertThat(guard.isExternalMemoryPressure()).isFalse();
            verify(flowController, times(1)).handleGlobalTransition(FlowTransition.TO_OPEN);
            verify(flowController, times(1)).wakeEligibleWaiters();
        }
    }

    @Test
    @DisplayName("shouldRequestGc returns true when allocated heap is high but live heap is low")
    void shouldRequestGc_highAllocatedLowLive_returnsTrue() {
        long mb = 1024 * 1024L;
        FlowControlProperties.Global global = new FlowControlProperties.Global(
                0.40, 0.75, 0.80, 256L, 1000L
        );
        FlowControlProperties props = new FlowControlProperties(true, 0L, global, 64);
        GlobalMemoryGuard guard = new GlobalMemoryGuard(1000L * mb, 500L * mb);
        MemorySnapshotProvider snapshotProvider = mock(MemorySnapshotProvider.class);
        MemoryPressureAdvisor advisor = mock(MemoryPressureAdvisor.class);

        try (MemoryPressureMonitor monitor = new MemoryPressureMonitor(props, guard, null, snapshotProvider, advisor)) {
            MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                    Instant.now(), 10L,
                    850 * mb, // 85% used >= 80%
                    900 * mb,
                    1000 * mb,
                    400 * mb,
                    300 * mb, // 30% live < 80%
                    10 * mb, 20 * mb, 1L, 10L, 0L, 1, 5,
                    2000 * mb, 4000 * mb
            );

            assertThat(monitor.shouldRequestGc(snapshot)).isTrue();

            // If cooldown has not elapsed
            monitor.setLastGcRequestTimeMs(System.currentTimeMillis());
            assertThat(monitor.shouldRequestGc(snapshot)).isFalse();

            // If live heap is also high (85%)
            monitor.setLastGcRequestTimeMs(0);
            MemoryMetricsSnapshot highLiveSnapshot = new MemoryMetricsSnapshot(
                    Instant.now(), 10L,
                    850 * mb,
                    900 * mb,
                    1000 * mb,
                    850 * mb,
                    850 * mb, // 85% live >= 80%
                    10 * mb, 20 * mb, 1L, 10L, 0L, 1, 5,
                    2000 * mb, 4000 * mb
            );
            assertThat(monitor.shouldRequestGc(highLiveSnapshot)).isFalse();
        }
    }

    @Test
    @DisplayName("disabled flow control does not run monitor")
    void disabledFlowControl_doesNotRun() {
        FlowControlProperties props = new FlowControlProperties(false, 0L, null, 64);
        GlobalMemoryGuard guard = new GlobalMemoryGuard(1000L, 500L);
        MemorySnapshotProvider snapshotProvider = mock(MemorySnapshotProvider.class);
        MemoryPressureAdvisor advisor = mock(MemoryPressureAdvisor.class);

        try (MemoryPressureMonitor monitor = new MemoryPressureMonitor(props, guard, null, snapshotProvider, advisor)) {
            assertThat(monitor.isRunning()).isFalse();
        }
    }
}
