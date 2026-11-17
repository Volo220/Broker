package com.volo.broker.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Off-heap {@link Payload} implementation backed by a Netty {@link ByteBuf}.
 */
public final class NettyDirectPayload implements Payload {

    private final ByteBuf buf;
    private final int size;
    private final @Nullable Runnable onZeroRef;

    public NettyDirectPayload(ByteBuf buf) {
        this(buf, null);
    }

    public NettyDirectPayload(ByteBuf buf, @Nullable Runnable onZeroRef) {
        this.buf = Objects.requireNonNull(buf, "buf must not be null");
        this.size = buf.readableBytes();
        this.onZeroRef = onZeroRef;
    }

    public ByteBuf byteBuf() {
        return buf;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public byte[] toByteArray() {
        byte[] bytes = new byte[size];
        buf.getBytes(0, bytes);
        return bytes;
    }

    @Override
    public ByteBuffer asReadOnlyByteBuffer() {
        return buf.nioBuffer(0, size).asReadOnlyBuffer();
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        writeTo(out, new byte[Math.min(size, 8192)]);
    }

    @Override
    public void writeTo(OutputStream out, byte[] scratch) throws IOException {
        Objects.requireNonNull(out, "out must not be null");
        byte[] effectiveScratch = (scratch == null || scratch.length == 0)
                ? new byte[Math.min(size, 8192)]
                : scratch;

        for (int offset = 0; offset < size; ) {
            int chunk = Math.min(effectiveScratch.length, size - offset);
            buf.getBytes(offset, effectiveScratch, 0, chunk);
            out.write(effectiveScratch, 0, chunk);
            offset += chunk;
        }
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer must not be null");
        buf.getBytes(0, buffer);
    }

    @Override
    public Payload retain() {
        buf.retain();
        return this;
    }

    @Override
    public boolean release() {
        boolean deallocated = buf.release();
        if (deallocated && onZeroRef != null) {
            onZeroRef.run();
        }
        return deallocated;
    }

    @Override
    public int refCnt() {
        return buf.refCnt();
    }

    @Override
    public boolean isReferenceCounted() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof NettyDirectPayload other) {
            return ByteBufUtil.equals(this.buf, 0, other.buf, 0, size);
        }
        if (o instanceof Payload other) {
            if (this.size() != other.size()) return false;
            return Arrays.equals(this.toByteArray(), other.toByteArray());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i = 0; i < size; i++) {
            result = 31 * result + buf.getByte(i);
        }
        return result;
    }

    @Override
    public String toString() {
        return "NettyDirectPayload[size=" + size + ", refCnt=" + refCnt() + "]";
    }
}
