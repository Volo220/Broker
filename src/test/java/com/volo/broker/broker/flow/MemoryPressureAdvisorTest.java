package com.volo.broker.broker.flow;

import com.volo.broker.config.FlowControlProperties;
import com.volo.broker.diagnostics.memory.MemoryMetricsSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class MemoryPressureAdvisorTest {

    private MemoryPressureAdvisor advisor;

    @BeforeEach
    void setUp() {
        advisor = new MemoryPressureAdvisor();
    }

    @Test
    void assess_calculatesWatermarksFromCeiling() {
        FlowControlProperties.Global config = new FlowControlProperties.Global(
                0.40,
                0.75,
                0.80,
                256L,
                500L
        );

        long mb = 1024 * 1024L;
        MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                Instant.now(),
                100L,
                500 * mb,
                1000 * mb,
                2000 * mb, // maxHeap = 2000 MB
                200 * mb,
                150 * mb,
                32 * mb,
                64 * mb,
                5L,
                120L,
                10 * mb,
                2,
                10,
                4000 * mb, // free OS = 4000 MB
                8000 * mb  // total OS = 8000 MB
        );

        MemoryAssessment assessment = advisor.assess(snapshot, config);

        // ceiling = min(2000 MB, 4000 + 500 MB) = 2000 MB
        // high = 2000 * 0.40 = 800 MB
        // low = 800 * 0.75 = 600 MB
        assertThat(assessment.effectiveDynamicHighBytes()).isEqualTo(800 * mb);
        assertThat(assessment.effectiveDynamicLowBytes()).isEqualTo(600 * mb);
        assertThat(assessment.underMemoryPressure()).isFalse();
        assertThat(assessment.pressureReason()).isEmpty();
    }

    @Test
    void assess_constrainedByOsMemory() {
        FlowControlProperties.Global config = new FlowControlProperties.Global(
                0.40,
                0.75,
                0.80,
                256L,
                500L
        );

        long mb = 1024 * 1024L;
        MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                Instant.now(),
                100L,
                100 * mb,
                200 * mb,
                2000 * mb, // maxHeap = 2000 MB
                50 * mb,
                40 * mb,
                10 * mb,
                20 * mb,
                1L,
                10L,
                0L,
                1,
                5,
                400 * mb, // free OS = 400 MB -> ceiling = 400 + 100 = 500 MB
                4000 * mb
        );

        MemoryAssessment assessment = advisor.assess(snapshot, config);

        // ceiling = 500 MB
        // high = 500 * 0.40 = 200 MB
        // low = 200 * 0.75 = 150 MB
        assertThat(assessment.effectiveDynamicHighBytes()).isEqualTo(200 * mb);
        assertThat(assessment.effectiveDynamicLowBytes()).isEqualTo(150 * mb);
        assertThat(assessment.underMemoryPressure()).isFalse();
    }

    @Test
    void assess_heapPressureExceeded_flagsUnderPressure() {
        FlowControlProperties.Global config = new FlowControlProperties.Global(
                0.40,
                0.75,
                0.80, // 80% threshold
                256L,
                500L
        );

        long mb = 1024 * 1024L;
        MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                Instant.now(),
                100L,
                900 * mb,
                950 * mb,
                1000 * mb, // maxHeap = 1000 MB
                850 * mb,
                850 * mb, // 85% live after GC >= 80%
                10 * mb,
                20 * mb,
                10L,
                200L,
                0L,
                1,
                5,
                2000 * mb,
                4000 * mb
        );

        MemoryAssessment assessment = advisor.assess(snapshot, config);

        assertThat(assessment.underMemoryPressure()).isTrue();
        assertThat(assessment.pressureReason()).contains("JVM_HEAP_PRESSURE: 85.0% live heap after GC >= threshold 80.0%");
    }

    @Test
    void assess_dirtyHeapWithLowLiveHeap_doesNotFlagPressure() {
        FlowControlProperties.Global config = new FlowControlProperties.Global(
                0.40,
                0.75,
                0.80, // 80% threshold
                256L,
                500L
        );

        long mb = 1024 * 1024L;
        // Heap used is 90% (dirty), but live heap after GC is only 30%
        MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                Instant.now(),
                100L,
                900 * mb, // 90% used
                950 * mb,
                1000 * mb, // maxHeap = 1000 MB
                400 * mb,
                300 * mb, // 30% live after GC < 80%
                10 * mb,
                20 * mb,
                10L,
                200L,
                0L,
                1,
                5,
                2000 * mb,
                4000 * mb
        );

        MemoryAssessment assessment = advisor.assess(snapshot, config);

        assertThat(assessment.underMemoryPressure()).isFalse();
        assertThat(assessment.pressureReason()).isEmpty();
    }

    @Test
    void assess_noGcRanYet_highAllocatedHeap_flagsPressure() {
        FlowControlProperties.Global config = new FlowControlProperties.Global(
                0.40,
                0.75,
                0.80, // 80% threshold
                256L,
                500L
        );

        long mb = 1024 * 1024L;
        MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                Instant.now(),
                100L,
                850 * mb, // 85% used
                900 * mb,
                1000 * mb,
                -1L,
                -1L, // No GC has run yet
                10 * mb,
                20 * mb,
                0L,
                0L,
                0L,
                1,
                5,
                2000 * mb,
                4000 * mb
        );

        MemoryAssessment assessment = advisor.assess(snapshot, config);

        assertThat(assessment.underMemoryPressure()).isTrue();
        assertThat(assessment.pressureReason()).contains("JVM_HEAP_PRESSURE: 85.0% used >= threshold 80.0%");
    }

    @Test
    void assess_hostMemoryExhaustion_flagsUnderPressure() {
        FlowControlProperties.Global config = new FlowControlProperties.Global(
                0.40,
                0.75,
                0.80,
                256L, // min 256 MB free OS
                500L
        );

        long mb = 1024 * 1024L;
        MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                Instant.now(),
                100L,
                200 * mb,
                400 * mb,
                1000 * mb,
                100 * mb,
                80 * mb,
                10 * mb,
                20 * mb,
                2L,
                20L,
                0L,
                1,
                5,
                100 * mb, // Only 100 MB free OS < 256 MB threshold
                4000 * mb
        );

        MemoryAssessment assessment = advisor.assess(snapshot, config);

        assertThat(assessment.underMemoryPressure()).isTrue();
        assertThat(assessment.pressureReason()).contains("HOST_MEMORY_EXHAUSTION: free OS RAM 100 MB <= min threshold 256 MB");
    }
}
