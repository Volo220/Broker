package com.volo.broker.buffer;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Heap-backed implementation of {@link PayloadAllocator} allocating standard {@link ByteArrayPayload}.
 */
public class HeapPayloadAllocator implements PayloadAllocator {

    @Override
    public Payload allocateFrom(InputStream in, int size) throws IOException {
        Objects.requireNonNull(in, "in must not be null");
        if (size <= 0) {
            return Payload.empty();
        }
        byte[] bytes = in.readNBytes(size);
        if (bytes.length < size) {
            throw new EOFException("Premature end of stream: expected " + size + " bytes, read " + bytes.length);
        }
        return Payload.wrap(bytes);
    }

    @Override
    public Payload allocate(int size, PayloadWriter writer) throws IOException {
        Objects.requireNonNull(writer, "writer must not be null");
        if (size <= 0) {
            return Payload.empty();
        }
        byte[] bytes = new byte[size];
        writer.write(ByteBuffer.wrap(bytes));
        return Payload.wrap(bytes);
    }

    @Override
    public long usedDirectBytes() {
        return 0L;
    }

    @Override
    public long liveBuffers() {
        return 0L;
    }
}
