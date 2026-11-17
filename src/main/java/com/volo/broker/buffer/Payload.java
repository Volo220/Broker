package com.volo.broker.buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Domain representation of message payload bytes, independent of transport protocols
 * and serialization formats.
 */
public interface Payload {

    /**
     * Size of the payload in bytes.
     */
    int size();

    /**
     * True if payload contains zero bytes.
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns a copy of the payload as a byte array.
     */
    byte[] toByteArray();

    /**
     * Read-only view of the payload as a {@link ByteBuffer}.
     */
    ByteBuffer asReadOnlyByteBuffer();

    /**
     * Writes payload contents to the specified output stream.
     */
    void writeTo(OutputStream out) throws IOException;

    /**
     * Writes payload contents into the specified destination buffer.
     */
    void writeTo(ByteBuffer buffer);

    /**
     * Writes payload contents in chunks using a reusable scratch buffer to eliminate heap allocations.
     */
    default void writeTo(OutputStream out, byte[] scratch) throws IOException {
        writeTo(out);
    }

    /**
     * Increments the reference count of this payload.
     * For heap-backed payloads, this is a no-op returning {@code this}.
     */
    default Payload retain() {
        return this;
    }

    /**
     * Decrements the reference count of this payload, releasing memory if count reaches zero.
     * For heap-backed payloads, this is a no-op returning {@code false}.
     */
    default boolean release() {
        return false;
    }

    /**
     * Returns current reference count. Defaults to 1 for unmanaged/heap payloads.
     */
    default int refCnt() {
        return 1;
    }

    /**
     * True if this payload requires explicit lifecycle management via {@link #release()}.
     */
    default boolean isReferenceCounted() {
        return false;
    }

    /**
     * Empty payload singleton.
     */
    static Payload empty() {
        return ByteArrayPayload.EMPTY;
    }

    /**
     * Creates an immutable heap payload by defensively copying the provided bytes.
     */
    static Payload of(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return ByteArrayPayload.EMPTY;
        }
        return new ByteArrayPayload(bytes);
    }

    /**
     * Creates an immutable heap payload from a ByteBuffer.
     */
    static Payload of(ByteBuffer buffer) {
        if (buffer == null || !buffer.hasRemaining()) {
            return ByteArrayPayload.EMPTY;
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return new ByteArrayPayload(bytes, true);
    }

    /**
     * Creates a heap payload wrapping the provided byte array without defensive copying.
     */
    static Payload wrap(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return ByteArrayPayload.EMPTY;
        }
        return new ByteArrayPayload(bytes, true);
    }
}
