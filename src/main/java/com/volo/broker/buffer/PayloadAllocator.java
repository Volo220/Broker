package com.volo.broker.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Port for allocating message payloads, supporting both heap and pooled off-heap strategies.
 */
public interface PayloadAllocator {

    /**
     * Allocates a payload by reading exactly {@code size} bytes from the given input stream.
     *
     * @param in source input stream
     * @param size exact payload size in bytes
     * @return allocated payload
     * @throws IOException on read or buffer allocation failure
     */
    Payload allocateFrom(InputStream in, int size) throws IOException;

    /**
     * Allocates a payload using a writer callback to populate the allocated destination buffer.
     *
     * @param size exact payload size in bytes
     * @param writer callback populating the destination buffer
     * @return allocated payload
     * @throws IOException on error during writing or allocation
     */
    Payload allocate(int size, PayloadWriter writer) throws IOException;

    /**
     * Returns total off-heap direct memory used by active allocated payloads in bytes,
     * or -1 if direct memory is not used.
     */
    long usedDirectBytes();

    /**
     * Returns the count of currently active (unreleased) direct payload buffers.
     */
    long liveBuffers();

    /**
     * Functional interface for writing directly into an allocated destination buffer.
     */
    @FunctionalInterface
    interface PayloadWriter {
        void write(ByteBuffer destination) throws IOException;
    }
}
