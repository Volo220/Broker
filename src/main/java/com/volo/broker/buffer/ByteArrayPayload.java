package com.volo.broker.buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Immutable heap-allocated {@link Payload} backed by a byte array.
 */
public final class ByteArrayPayload implements Payload {

    public static final ByteArrayPayload EMPTY = new ByteArrayPayload(new byte[0], true);

    private final byte[] bytes;

    public ByteArrayPayload(byte[] bytes) {
        this.bytes = bytes == null ? new byte[0] : bytes.clone();
    }

    ByteArrayPayload(byte[] bytes, boolean trusted) {
        this.bytes = (trusted || bytes == null) ? (bytes == null ? new byte[0] : bytes) : bytes.clone();
    }

    @Override
    public int size() {
        return bytes.length;
    }

    @Override
    public byte[] toByteArray() {
        return bytes.clone();
    }

    @Override
    public ByteBuffer asReadOnlyByteBuffer() {
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(bytes);
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof ByteArrayPayload other) {
            return Arrays.equals(bytes, other.bytes);
        }
        if (o instanceof Payload other) {
            if (this.size() != other.size()) return false;
            return Arrays.equals(this.bytes, other.toByteArray());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "ByteArrayPayload[size=" + size() + "]";
    }
}
