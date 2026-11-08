package com.volo.broker.diagnostics.memory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CsvMetricsWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCreateFileWithHeaderAndAppendRows() throws IOException {
        Path csvPath = tempDir.resolve("metrics/output.csv");

        try (CsvMetricsWriter writer = new CsvMetricsWriter(csvPath.toString(), false)) {
            MemoryMetricsSnapshot snapshot = new MemoryMetricsSnapshot(
                    Instant.parse("2026-11-03T20:00:00Z"),
                    10L,
                    500 * 1024 * 1024L,
                    1000 * 1024 * 1024L,
                    2000 * 1024 * 1024L,
                    200 * 1024 * 1024L,
                    150 * 1024 * 1024L,
                    32 * 1024 * 1024L,
                    64 * 1024 * 1024L,
                    5L,
                    120L,
                    10 * 1024 * 1024L,
                    3,
                    42
            );

            writer.writeSnapshot(snapshot);
        }

        assertThat(Files.exists(csvPath)).isTrue();
        List<String> lines = Files.readAllLines(csvPath);
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).isEqualTo(CsvMetricsWriter.CSV_HEADER);
        assertThat(lines.get(1)).contains("2026-11-03T20:00:00Z,10,500.0,1000.0,2000.0,200.0,150.0,32.0,64.0,5,120,10.0,3,42");
    }

    @Test
    void appendMode_shouldPreserveExistingContentWithoutDuplicateHeader() throws IOException {
        Path csvPath = tempDir.resolve("append.csv");

        // Write first snapshot with append=false
        try (CsvMetricsWriter writer = new CsvMetricsWriter(csvPath.toString(), false)) {
            writer.writeSnapshot(new MemoryMetricsSnapshot(
                    Instant.parse("2026-11-03T20:00:01Z"),
                    1L, 100L, 200L, 300L, 50L, 40L, 10L, 20L, 1L, 10L, 5L, 1, 10
            ));
        }

        // Reopen with append=true and write second snapshot
        try (CsvMetricsWriter writer = new CsvMetricsWriter(csvPath.toString(), true)) {
            writer.writeSnapshot(new MemoryMetricsSnapshot(
                    Instant.parse("2026-11-03T20:00:02Z"),
                    2L, 110L, 200L, 300L, 50L, 40L, 10L, 20L, 1L, 10L, 5L, 1, 10
            ));
        }

        List<String> lines = Files.readAllLines(csvPath);
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo(CsvMetricsWriter.CSV_HEADER);
        assertThat(lines.get(1)).contains("2026-11-03T20:00:01Z");
        assertThat(lines.get(2)).contains("2026-11-03T20:00:02Z");
    }
}

