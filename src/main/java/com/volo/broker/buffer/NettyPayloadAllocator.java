package com.volo.broker.buffer;

import io.grpc.HasByteBuffer;
import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * Netty-backed implementation of {@link PayloadAllocator} using direct off-heap memory.
 */
public class NettyPayloadAllocator implements PayloadAllocator {

    private final ByteBufAllocator allocator;
    private final int inlineThresholdBytes;
    private final LongAdder liveBuffers = new LongAdder();
    private final LongAdder usedDirectBytes = new LongAdder();

    public NettyPayloadAllocator(int inlineThresholdBytes) {
        this(new AdaptiveByteBufAllocator(true), inlineThresholdBytes);
    }

    public NettyPayloadAllocator(ByteBufAllocator allocator, int inlineThresholdBytes) {
        this.allocator = Objects.requireNonNull(allocator, "allocator must not be null");
        this.inlineThresholdBytes = Math.max(0, inlineThresholdBytes);
    }

    @Override
    public Payload allocateFrom(InputStream in, int size) throws IOException {
        Objects.requireNonNull(in, "in must not be null");
        if (size <= 0) {
            return Payload.empty();
        }
        if (size <= inlineThresholdBytes) {
            return readInlineHeapPayload(in, size);
        }

        ByteBuf buf = allocator.directBuffer(size, size);
        try {
            fillFromStream(buf, in, size);
        } catch (Throwable t) {
            buf.release();
            throw (t instanceof IOException ioe) ? ioe : new IOException("Failed to populate direct payload", t);
        }

        return wrapDirectBuffer(buf, size);
    }

    @Override
    public Payload allocate(int size, PayloadWriter writer) throws IOException {
        Objects.requireNonNull(writer, "writer must not be null");
        if (size <= 0) {
            return Payload.empty();
        }
        if (size <= inlineThresholdBytes) {
            byte[] bytes = new byte[size];
            writer.write(ByteBuffer.wrap(bytes));
            return Payload.wrap(bytes);
        }

        ByteBuf buf = allocator.directBuffer(size, size);
        try {
            ByteBuffer nio = buf.nioBuffer(0, size);
            writer.write(nio);
            buf.writerIndex(size);
        } catch (Throwable t) {
            buf.release();
            throw (t instanceof IOException ioe) ? ioe : new IOException("Failed to write to direct payload", t);
        }

        return wrapDirectBuffer(buf, size);
    }

    @Override
    public long usedDirectBytes() {
        return usedDirectBytes.sum();
    }

    @Override
    public long liveBuffers() {
        return liveBuffers.sum();
    }

    public ByteBufAllocator allocator() {
        return allocator;
    }

    public int inlineThresholdBytes() {
        return inlineThresholdBytes;
    }

    private Payload wrapDirectBuffer(ByteBuf buf, int size) {
        liveBuffers.increment();
        usedDirectBytes.add(size);
        return new NettyDirectPayload(buf, () -> {
            liveBuffers.decrement();
            usedDirectBytes.add(-size);
        });
    }

    private Payload readInlineHeapPayload(InputStream in, int size) throws IOException {
        byte[] bytes = in.readNBytes(size);
        if (bytes.length < size) {
            throw new EOFException("Premature end of stream: expected " + size + " bytes, read " + bytes.length);
        }
        return Payload.wrap(bytes);
    }

    private void fillFromStream(ByteBuf buf, InputStream in, int size) throws IOException {
        if (in instanceof HasByteBuffer hb && hb.byteBufferSupported()) {
            readFromHasByteBuffer(buf, hb, in, size);
        } else {
            readFromStandardStream(buf, in, size);
        }
    }

    private void readFromHasByteBuffer(ByteBuf buf, HasByteBuffer hb, InputStream in, int size) throws IOException {
        int remaining = size;
        while (remaining > 0) {
            ByteBuffer src = hb.getByteBuffer();
            if (src == null || !src.hasRemaining()) {
                break;
            }
            int chunk = Math.min(src.remaining(), remaining);
            buf.writeBytes(src.slice(src.position(), chunk));
            in.skip(chunk);
            remaining -= chunk;
        }
        if (remaining > 0) {
            throw new EOFException("Incomplete HasByteBuffer stream: missing " + remaining + " bytes");
        }
    }

    private void readFromStandardStream(ByteBuf buf, InputStream in, int size) throws IOException {
        byte[] scratch = new byte[Math.min(size, 8192)];
        int remaining = size;
        while (remaining > 0) {
            int toRead = Math.min(scratch.length, remaining);
            int read = in.read(scratch, 0, toRead);
            if (read == -1) {
                throw new EOFException("Premature end of stream: expected " + size + " bytes, missing " + remaining);
            }
            buf.writeBytes(scratch, 0, read);
            remaining -= read;
        }
    }
}
