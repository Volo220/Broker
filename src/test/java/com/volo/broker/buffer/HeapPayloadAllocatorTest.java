package com.volo.broker.buffer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class HeapPayloadAllocatorTest {

    @Test
    void allocateFrom_shouldAlwaysAllocateByteArrayPayload() throws IOException {
        HeapPayloadAllocator allocator = new HeapPayloadAllocator();
        byte[] data = "heap-test-payload".getBytes(StandardCharsets.UTF_8);

        Payload payload = allocator.allocateFrom(new ByteArrayInputStream(data), data.length);

        assertThat(payload).isInstanceOf(ByteArrayPayload.class);
        assertThat(payload.isReferenceCounted()).isFalse();
        assertThat(payload.toByteArray()).isEqualTo(data);
        assertThat(allocator.usedDirectBytes()).isZero();
        assertThat(allocator.liveBuffers()).isZero();
    }

    @Test
    void allocateWithWriter_shouldAllocateByteArrayPayload() throws IOException {
        HeapPayloadAllocator allocator = new HeapPayloadAllocator();
        byte[] data = "writer-heap-data".getBytes(StandardCharsets.UTF_8);

        Payload payload = allocator.allocate(data.length, buf -> buf.put(data));

        assertThat(payload).isInstanceOf(ByteArrayPayload.class);
        assertThat(payload.toByteArray()).isEqualTo(data);
    }
}
