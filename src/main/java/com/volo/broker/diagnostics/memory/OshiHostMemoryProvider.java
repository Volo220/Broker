package com.volo.broker.diagnostics.memory;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;

/**
 * OSHI-backed {@link HostMemoryProvider} reading OS-reported physical memory
 * availability (e.g. {@code MemAvailable} on Linux, available RAM on Windows/macOS).
 */
@Component
public class OshiHostMemoryProvider implements HostMemoryProvider {

    private static final Logger log = LoggerFactory.getLogger(OshiHostMemoryProvider.class);

    private final @Nullable GlobalMemory globalMemory;

    @Autowired
    public OshiHostMemoryProvider() {
        this(initGlobalMemory());
    }

    OshiHostMemoryProvider(@Nullable GlobalMemory globalMemory) {
        this.globalMemory = globalMemory;
    }

    @Override
    public HostMemorySnapshot read() {
        if (globalMemory == null) {
            return HostMemorySnapshot.UNAVAILABLE;
        }
        try {
            long availableBytes = globalMemory.getAvailable();
            long totalBytes = globalMemory.getTotal();
            if (totalBytes <= 0 || availableBytes < 0) {
                return HostMemorySnapshot.UNAVAILABLE;
            }
            return new HostMemorySnapshot(availableBytes, totalBytes);
        } catch (Throwable ex) {
            log.debug("Failed to read host memory metrics via OSHI", ex);
            return HostMemorySnapshot.UNAVAILABLE;
        }
    }

    private static @Nullable GlobalMemory initGlobalMemory() {
        try {
            return new SystemInfo().getHardware().getMemory();
        } catch (Throwable ex) {
            log.warn("Failed to initialize OSHI host memory provider; host memory metrics will be unavailable", ex);
            return null;
        }
    }
}
