package com.volo.broker.buffer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NettyPayloadAllocatorTest {

    @Test
    void allocateFrom_shouldReturnEmptyWhenSizeIsZero() throws IOException {
        NettyPayloadAllocator allocator = new NettyPayloadAllocator(256);
        Payload payload = allocator.allocateFrom(new ByteArrayInputStream(new byte[0]), 0);

        assertThat(payload.isEmpty()).isTrue();
        assertThat(payload.isReferenceCounted()).isFalse();
    }

    @Test
    void allocateFrom_shouldAllocateInlineHeapWhenBelowThreshold() throws IOException {
        NettyPayloadAllocator allocator = new NettyPayloadAllocator(256);
        byte[] data = "small payload".getBytes(StandardCharsets.UTF_8);

        Payload payload = allocator.allocateFrom(new ByteArrayInputStream(data), data.length);

        assertThat(payload).isInstanceOf(ByteArrayPayload.class);
        assertThat(payload.isReferenceCounted()).isFalse();
        assertThat(payload.toByteArray()).isEqualTo(data);
        assertThat(allocator.usedDirectBytes()).isZero();
        assertThat(allocator.liveBuffers()).isZero();
    }

    @Test
    void allocateFrom_shouldAllocateDirectWhenAboveThreshold() throws IOException {
        NettyPayloadAllocator allocator = new NettyPayloadAllocator(16);
        byte[] data = "larger payload exceeding inline threshold".getBytes(StandardCharsets.UTF_8);

        Payload payload = allocator.allocateFrom(new ByteArrayInputStream(data), data.length);

        assertThat(payload).isInstanceOf(NettyDirectPayload.class);
        assertThat(payload.isReferenceCounted()).isTrue();
        assertThat(payload.toByteArray()).isEqualTo(data);
        assertThat(allocator.usedDirectBytes()).isEqualTo(data.length);
        assertThat(allocator.liveBuffers()).isEqualTo(1);

        payload.release();
        assertThat(allocator.usedDirectBytes()).isZero();
        assertThat(allocator.liveBuffers()).isZero();
    }

    @Test
    void allocateFrom_shouldThrowEofWhenStreamIsPrematurelyTruncated() {
        NettyPayloadAllocator allocator = new NettyPayloadAllocator(16);
        byte[] data = "short".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> allocator.allocateFrom(new ByteArrayInputStream(data), 100))
                .isInstanceOf(EOFException.class);
        assertThat(allocator.usedDirectBytes()).isZero();
        assertThat(allocator.liveBuffers()).isZero();
    }

    @Test
    void allocateWithWriter_shouldSupportInlineAndDirect() throws IOException {
        NettyPayloadAllocator allocator = new NettyPayloadAllocator(16);

        // Small via writer
        Payload small = allocator.allocate(5, buf -> buf.put("hello".getBytes(StandardCharsets.UTF_8)));
        assertThat(small).isInstanceOf(ByteArrayPayload.class);
        assertThat(small.toByteArray()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));

        // Direct via writer
        byte[] largeData = "direct-writer-test-payload".getBytes(StandardCharsets.UTF_8);
        Payload direct = allocator.allocate(largeData.length, buf -> buf.put(largeData));
        assertThat(direct).isInstanceOf(NettyDirectPayload.class);
        assertThat(direct.toByteArray()).isEqualTo(largeData);
        assertThat(allocator.usedDirectBytes()).isEqualTo(largeData.length);

        direct.release();
        assertThat(allocator.usedDirectBytes()).isZero();
    }
}
