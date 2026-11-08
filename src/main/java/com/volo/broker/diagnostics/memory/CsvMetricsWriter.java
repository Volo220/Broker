package com.volo.broker.diagnostics.memory;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class CsvMetricsWriter implements Closeable {

    public static final String CSV_HEADER =
            "timestamp,uptime_sec,heap_used_mb,heap_committed_mb,heap_max_mb,old_gen_used_mb,old_gen_after_gc_mb,direct_used_mb,direct_capacity_mb,gc_count,gc_time_ms,broker_ready_mb,total_queues,live_threads";

    private final Path filePath;
    private final boolean append;
    private BufferedWriter writer;

    public CsvMetricsWriter(String filePath, boolean append) throws IOException {
        Objects.requireNonNull(filePath, "filePath must not be null");
        this.filePath = Paths.get(filePath);
        this.append = append;
        init();
    }

    private void init() throws IOException {
        Path parent = filePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        boolean fileExists = Files.exists(filePath);
        boolean fileHasContent = fileExists && Files.size(filePath) > 0;

        StandardOpenOption[] options = append
                ? new StandardOpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND}
                : new StandardOpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};

        this.writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8, options);

        if (!append || !fileHasContent) {
            writer.write(CSV_HEADER);
            writer.newLine();
            writer.flush();
        }
    }

    public synchronized void writeSnapshot(MemoryMetricsSnapshot snapshot) throws IOException {
        if (writer == null || snapshot == null) {
            return;
        }

        String line = String.format(
                "%s,%d,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%d,%d,%.1f,%d,%d",
                snapshot.timestamp(),
                snapshot.uptimeSec(),
                snapshot.heapUsedMb(),
                snapshot.heapCommittedMb(),
                snapshot.heapMaxMb(),
                snapshot.oldGenUsedMb(),
                snapshot.oldGenAfterGcMb(),
                snapshot.directMemoryUsedMb(),
                snapshot.directMemoryCapacityMb(),
                snapshot.gcCount(),
                snapshot.gcTimeMs(),
                snapshot.brokerReadyMb(),
                snapshot.totalQueues(),
                snapshot.liveThreads()
        );

        writer.write(line);
        writer.newLine();
        writer.flush();
    }

    public synchronized void flush() throws IOException {
        if (writer != null) {
            writer.flush();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (writer != null) {
            try {
                writer.flush();
            } finally {
                writer.close();
                writer = null;
            }
        }
    }

    public Path getFilePath() {
        return filePath;
    }
}

