package com.volo.broker.buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test implementation of {@link Payload} tracking retain/release calls
 * and detecting use-after-free or double-release bugs.
 */
public class TestCountingPayload implements Payload {

    private final byte[] bytes;
    private final AtomicInteger refCnt;
    private final AtomicInteger retainCount = new AtomicInteger();
    private final AtomicInteger releaseCount = new AtomicInteger();

    public TestCountingPayload(byte[] bytes) {
        this(bytes, 1);
    }

    public TestCountingPayload(byte[] bytes, int initialRef) {
        this.bytes = bytes == null ? new byte[0] : bytes.clone();
        this.refCnt = new AtomicInteger(initialRef);
    }

    @Override
    public int size() {
        ensureNotFreed();
        return bytes.length;
    }

    @Override
    public byte[] toByteArray() {
        ensureNotFreed();
        return bytes.clone();
    }

    @Override
    public ByteBuffer asReadOnlyByteBuffer() {
        ensureNotFreed();
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        ensureNotFreed();
        out.write(bytes);
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        ensureNotFreed();
        buffer.put(bytes);
    }

    @Override
    public Payload retain() {
        int current = refCnt.get();
        if (current <= 0) {
            throw new IllegalStateException("Cannot retain already released payload (refCnt=" + current + ")");
        }
        refCnt.incrementAndGet();
        retainCount.incrementAndGet();
        return this;
    }

    @Override
    public boolean release() {
        int current = refCnt.get();
        if (current <= 0) {
            throw new IllegalStateException("Double release detected (refCnt=" + current + ")");
        }
        releaseCount.incrementAndGet();
        return refCnt.decrementAndGet() == 0;
    }

    @Override
    public int refCnt() {
        return refCnt.get();
    }

    @Override
    public boolean isReferenceCounted() {
        return true;
    }

    public int retainCalls() {
        return retainCount.get();
    }

    public int releaseCalls() {
        return releaseCount.get();
    }

    private void ensureNotFreed() {
        if (refCnt.get() <= 0) {
            throw new IllegalStateException("Cannot access payload after release (refCnt=" + refCnt.get() + ")");
        }
    }
}
