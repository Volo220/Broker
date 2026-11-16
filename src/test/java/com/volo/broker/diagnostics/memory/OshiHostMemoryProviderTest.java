package com.volo.broker.diagnostics.memory;

import com.volo.broker.diagnostics.memory.HostMemoryProvider.HostMemorySnapshot;
import org.junit.jupiter.api.Test;
import oshi.hardware.GlobalMemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OshiHostMemoryProviderTest {

    @Test
    void read_returnsAvailableAndTotalBytesFromGlobalMemory() {
        GlobalMemory memory = mock(GlobalMemory.class);
        when(memory.getAvailable()).thenReturn(8_000_000_000L);
        when(memory.getTotal()).thenReturn(16_000_000_000L);

        OshiHostMemoryProvider provider = new OshiHostMemoryProvider(memory);
        HostMemorySnapshot snapshot = provider.read();

        assertThat(snapshot.availableBytes()).isEqualTo(8_000_000_000L);
        assertThat(snapshot.totalBytes()).isEqualTo(16_000_000_000L);
    }

    @Test
    void read_whenGlobalMemoryIsNullOrThrows_returnsUnavailable() {
        assertThat(new OshiHostMemoryProvider(null).read()).isEqualTo(HostMemorySnapshot.UNAVAILABLE);

        GlobalMemory failingMemory = mock(GlobalMemory.class);
        when(failingMemory.getAvailable()).thenThrow(new RuntimeException("oshi error"));
        assertThat(new OshiHostMemoryProvider(failingMemory).read()).isEqualTo(HostMemorySnapshot.UNAVAILABLE);
    }

    @Test
    void read_whenValuesAreInvalid_returnsUnavailable() {
        GlobalMemory invalidTotal = mock(GlobalMemory.class);
        when(invalidTotal.getAvailable()).thenReturn(1024L);
        when(invalidTotal.getTotal()).thenReturn(0L);
        assertThat(new OshiHostMemoryProvider(invalidTotal).read()).isEqualTo(HostMemorySnapshot.UNAVAILABLE);

        GlobalMemory negativeAvailable = mock(GlobalMemory.class);
        when(negativeAvailable.getAvailable()).thenReturn(-1L);
        when(negativeAvailable.getTotal()).thenReturn(16_000_000_000L);
        assertThat(new OshiHostMemoryProvider(negativeAvailable).read()).isEqualTo(HostMemorySnapshot.UNAVAILABLE);
    }

    @Test
    void read_liveSystemInfo_returnsPositivePhysicalMemory() {
        OshiHostMemoryProvider provider = new OshiHostMemoryProvider();
        HostMemorySnapshot snapshot = provider.read();

        assertThat(snapshot.totalBytes()).isPositive();
        assertThat(snapshot.availableBytes()).isGreaterThanOrEqualTo(0L);
        assertThat(snapshot.availableBytes()).isLessThanOrEqualTo(snapshot.totalBytes());
    }
}
