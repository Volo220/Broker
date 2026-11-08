package com.volo.broker.diagnostics.memory;

import com.volo.broker.config.MemorySamplerProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class MemorySamplerServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void disabledSampler_shouldNotStartOrWriteFiles() {
        MemorySamplerProperties props = new MemorySamplerProperties(
                false,
                100,
                new MemorySamplerProperties.CsvConfig(true, tempDir.resolve("disabled.csv").toString(), false),
                new MemorySamplerProperties.ConsoleConfig(false, 1),
                null
        );

        MemoryMetricsCollector collector = mock(MemoryMetricsCollector.class);
        MemorySamplerService service = new MemorySamplerService(props, collector);

        service.start();

        assertThat(service.isRunning()).isFalse();
        verifyNoInteractions(collector);
        assertThat(Files.exists(tempDir.resolve("disabled.csv"))).isFalse();

        service.close();
    }

    @Test
    void enabledSampler_shouldSampleAndWriteToCsv() throws IOException {
        Path csvPath = tempDir.resolve("active.csv");
        MemorySamplerProperties props = new MemorySamplerProperties(
                true,
                50,
                new MemorySamplerProperties.CsvConfig(true, csvPath.toString(), false),
                new MemorySamplerProperties.ConsoleConfig(true, 1),
                null
        );

        MemoryMetricsCollector collector = mock(MemoryMetricsCollector.class);
        when(collector.collect()).thenReturn(new MemoryMetricsSnapshot(
                Instant.now(), 5L, 100L, 200L, 300L, 50L, 40L, 10L, 20L, 1L, 10L, 0L, 0, 5
        ));

        MemorySamplerService service = new MemorySamplerService(props, collector);
        service.start();

        assertThat(service.isRunning()).isTrue();

        service.sample();
        service.close();

        assertThat(service.isRunning()).isFalse();
        assertThat(Files.exists(csvPath)).isTrue();
        assertThat(Files.readAllLines(csvPath).size()).isGreaterThanOrEqualTo(2);
    }
}

