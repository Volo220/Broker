package com.volo.broker.broker.flow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks and logs flow control transition and timeout statistics.
 */
class FlowControlMetrics {

    private static final Logger log = LoggerFactory.getLogger(FlowControlMetrics.class);

    private final AtomicLong totalTransitionsToBlocked = new AtomicLong();
    private final AtomicLong totalTransitionsToOpen = new AtomicLong();
    private final AtomicLong overdueTimeoutRejections = new AtomicLong();

    void recordBlocked() {
        totalTransitionsToBlocked.incrementAndGet();
    }

    void recordOpen() {
        totalTransitionsToOpen.incrementAndGet();
    }

    void recordOverdueRejection() {
        overdueTimeoutRejections.incrementAndGet();
    }

    long totalTransitionsToBlocked() {
        return totalTransitionsToBlocked.get();
    }

    long totalTransitionsToOpen() {
        return totalTransitionsToOpen.get();
    }

    long overdueTimeoutRejections() {
        return overdueTimeoutRejections.get();
    }

    void logShutdownSummary() {
        log.info("""
                =======================================================
                [Flow Control Shutdown] Statistics:
                  - Total queue transitions to BLOCKED : {}
                  - Total queue transitions to OPEN    : {}
                  - Overdue timeout rejections         : {}
                =======================================================""",
                totalTransitionsToBlocked.get(),
                totalTransitionsToOpen.get(),
                overdueTimeoutRejections.get()
        );
    }
}
