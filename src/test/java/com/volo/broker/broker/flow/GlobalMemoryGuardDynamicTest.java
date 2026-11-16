package com.volo.broker.broker.flow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalMemoryGuardDynamicTest {

    private GlobalMemoryGuard guard;

    @BeforeEach
    void setUp() {
        guard = new GlobalMemoryGuard(1000L * 1024 * 1024, 500L * 1024 * 1024);
    }

    @Test
    @DisplayName("External memory pressure transitions to BLOCKED and back to OPEN")
    void externalPressureTransitions() {
        assertThat(guard.isBlocked()).isFalse();

        // Trigger external pressure (e.g. host RAM low)
        FlowTransition t1 = guard.updateExternalPressure(true, "HOST_MEMORY_EXHAUSTION: free OS RAM 100 MB <= 256 MB");
        assertThat(t1).isEqualTo(FlowTransition.TO_BLOCKED);
        assertThat(guard.isBlocked()).isTrue();
        assertThat(guard.isExternalMemoryPressure()).isTrue();
        assertThat(guard.currentBlockReason()).contains("HOST_MEMORY_EXHAUSTION");

        // Further additions remain blocked
        FlowTransition t2 = guard.recordBytesAdded(100);
        assertThat(t2).isEqualTo(FlowTransition.NONE);
        assertThat(guard.isBlocked()).isTrue();

        // Byte removal cannot unblock while external pressure is active
        FlowTransition t3 = guard.recordBytesRemoved(100);
        assertThat(t3).isEqualTo(FlowTransition.NONE);
        assertThat(guard.isBlocked()).isTrue();

        // Release external pressure
        FlowTransition t4 = guard.updateExternalPressure(false, "");
        assertThat(t4).isEqualTo(FlowTransition.TO_OPEN);
        assertThat(guard.isBlocked()).isFalse();
        assertThat(guard.isExternalMemoryPressure()).isFalse();
        assertThat(guard.currentBlockReason()).isEmpty();
    }

    @Test
    @DisplayName("Dynamic watermarks adaptation triggers BLOCKED if current bytes exceed new HWM")
    void dynamicWatermarksAdaptation() {
        long mb = 1024 * 1024L;
        // Start with high = 1000 MB, low = 500 MB
        guard.recordBytesAdded(400 * mb);
        assertThat(guard.isBlocked()).isFalse();

        // Suddenly system memory drops, so new HWM becomes 300 MB, LWM 200 MB
        FlowTransition t1 = guard.updateDynamicWatermarks(300 * mb, 200 * mb);
        assertThat(t1).isEqualTo(FlowTransition.TO_BLOCKED);
        assertThat(guard.isBlocked()).isTrue();
        assertThat(guard.effectiveHighBytes()).isEqualTo(300 * mb);
        assertThat(guard.effectiveLowBytes()).isEqualTo(200 * mb);

        // System memory recovers, new HWM becomes 800 MB, LWM 600 MB
        // Since current = 400 MB <= 600 MB LWM, should unblock!
        FlowTransition t2 = guard.updateDynamicWatermarks(800 * mb, 600 * mb);
        assertThat(t2).isEqualTo(FlowTransition.TO_OPEN);
        assertThat(guard.isBlocked()).isFalse();
    }
}
