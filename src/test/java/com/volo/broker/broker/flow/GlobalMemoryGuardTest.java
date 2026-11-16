package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalMemoryGuardTest {

    @Test
    @DisplayName("recordBytesAdded and removed follow hysteresis between high and low limits")
    void globalGuardHysteresis() {
        GlobalMemoryGuard guard = new GlobalMemoryGuard(1000L, 500L);
        assertThat(guard.isBlocked()).isFalse();
        assertThat(guard.globalState()).isEqualTo(FlowState.OPEN);

        // Add 800 bytes -> < 1000 -> NONE
        FlowTransition t1 = guard.recordBytesAdded(800);
        assertThat(t1).isEqualTo(FlowTransition.NONE);
        assertThat(guard.isBlocked()).isFalse();

        // Add 250 bytes -> total = 1050 >= 1000 -> TO_BLOCKED
        FlowTransition t2 = guard.recordBytesAdded(250);
        assertThat(t2).isEqualTo(FlowTransition.TO_BLOCKED);
        assertThat(guard.isBlocked()).isTrue();
        assertThat(guard.globalState()).isEqualTo(FlowState.BLOCKED);

        // Add more bytes -> keeps BLOCKED, NONE
        FlowTransition t3 = guard.recordBytesAdded(100);
        assertThat(t3).isEqualTo(FlowTransition.NONE);
        assertThat(guard.isBlocked()).isTrue();

        // Remove 400 bytes -> total = 750 (still > 500 lowBytes) -> keeps BLOCKED, NONE
        FlowTransition t4 = guard.recordBytesRemoved(400);
        assertThat(t4).isEqualTo(FlowTransition.NONE);
        assertThat(guard.isBlocked()).isTrue();

        // Remove 300 bytes -> total = 450 (<= 500 lowBytes) -> TO_OPEN
        FlowTransition t5 = guard.recordBytesRemoved(300);
        assertThat(t5).isEqualTo(FlowTransition.TO_OPEN);
        assertThat(guard.isBlocked()).isFalse();
        assertThat(guard.globalState()).isEqualTo(FlowState.OPEN);
    }

    @Test
    @DisplayName("disabled flow control never blocks")
    void disabledGuardNeverBlocks() {
        FlowControlProperties props = new FlowControlProperties(false, 0L, null, 64);
        GlobalMemoryGuard guard = new GlobalMemoryGuard(props);

        FlowTransition t1 = guard.recordBytesAdded(10_000_000_000L);
        assertThat(t1).isEqualTo(FlowTransition.NONE);
        assertThat(guard.isBlocked()).isFalse();
    }
}

