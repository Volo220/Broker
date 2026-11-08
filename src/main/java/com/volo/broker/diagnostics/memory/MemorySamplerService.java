package com.volo.broker.diagnostics.memory;

import com.volo.broker.config.MemorySamplerProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class MemorySamplerService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MemorySamplerService.class);

    private final MemorySamplerProperties properties;
    private final MemoryMetricsCollector collector;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sampleIndex = new AtomicLong(0);

    private ScheduledExecutorService executor;
    private CsvMetricsWriter csvWriter;

    @Autowired
    public MemorySamplerService(
            MemorySamplerProperties properties,
            MemoryMetricsCollector collector
    ) {
        this.properties = properties != null ? properties : new MemorySamplerProperties(false, 1000, null, null, null);
        this.collector = collector;
    }

    @PostConstruct
    public synchronized void start() {
        if (!properties.enabled()) {
            log.debug("Memory sampler is disabled by configuration");
            return;
        }

        if (running.get()) {
            return;
        }

        if (properties.csv().enabled()) {
            try {
                this.csvWriter = new CsvMetricsWriter(
                        properties.csv().filePath(),
                        properties.csv().append()
                );
                log.info("Memory sampler writing CSV metrics to: {}", csvWriter.getFilePath().toAbsolutePath());
            } catch (IOException e) {
                log.error("Failed to initialize memory sampler CSV writer", e);
            }
        }

        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "memory-sampler");
            t.setDaemon(true);
            return t;
        });

        long intervalMs = properties.sampleIntervalMs();
        this.executor.scheduleAtFixedRate(this::safeSample, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        this.running.set(true);

        log.info("Memory sampler started (interval: {} ms, console: {} every {}s)",
                intervalMs, properties.console().enabled(), properties.console().intervalSeconds());
    }

    public void sample() {
        MemoryMetricsSnapshot snapshot = collector.collect();

        if (csvWriter != null) {
            try {
                csvWriter.writeSnapshot(snapshot);
            } catch (IOException e) {
                log.warn("Failed to write memory metrics snapshot to CSV: {}", e.getMessage());
            }
        }

        long idx = sampleIndex.incrementAndGet();
        if (properties.console().enabled()) {
            long consoleIntervalSamples = Math.max(1, (properties.console().intervalSeconds() * 1000L) / properties.sampleIntervalMs());
            if (idx % consoleIntervalSamples == 0) {
                logConsoleSnapshot(snapshot);
            }
        }
    }

    private void safeSample() {
        try {
            sample();
        } catch (Throwable throwable) {
            log.warn("Unexpected error in memory sampler tick", throwable);
        }
    }

    private void logConsoleSnapshot(MemoryMetricsSnapshot snapshot) {
        double liveMb = snapshot.oldGenAfterGcMb() >= 0
                ? snapshot.oldGenAfterGcMb()
                : snapshot.oldGenUsedMb();

        log.info("[MEM] Heap: {}/{} MB (Max: {} MB) | OldGen Live: {} MB | Direct: {} MB | GC: {} ({} ms) | Backlog: {} MB | Queues: {} | Threads: {}",
                snapshot.heapUsedMb(),
                snapshot.heapCommittedMb(),
                snapshot.heapMaxMb(),
                liveMb >= 0 ? liveMb : 0.0,
                snapshot.directMemoryUsedMb() >= 0 ? snapshot.directMemoryUsedMb() : 0.0,
                snapshot.gcCount() >= 0 ? snapshot.gcCount() : 0,
                snapshot.gcTimeMs() >= 0 ? snapshot.gcTimeMs() : 0,
                snapshot.brokerReadyMb() >= 0 ? snapshot.brokerReadyMb() : 0.0,
                snapshot.totalQueues() >= 0 ? snapshot.totalQueues() : 0,
                snapshot.liveThreads() >= 0 ? snapshot.liveThreads() : 0
        );
    }

    @Override
    @PreDestroy
    public synchronized void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }

        if (csvWriter != null) {
            try {
                csvWriter.close();
            } catch (IOException e) {
                log.warn("Error closing memory sampler CSV writer: {}", e.getMessage());
            }
            csvWriter = null;
        }

        log.info("Memory sampler stopped");
    }

    public boolean isRunning() {
        return running.get();
    }

    public MemorySamplerProperties getProperties() {
        return properties;
    }
}

