package com.volo.broker.buffer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class PayloadTest {

    @Test
    void emptyPayload_shouldBeSingletonAndZeroSize() {
        Payload p1 = Payload.empty();
        Payload p2 = Payload.of(new byte[0]);
        Payload p3 = Payload.wrap(new byte[0]);
        Payload p4 = Payload.of(ByteBuffer.allocate(0));

        assertThat(p1.isEmpty()).isTrue();
        assertThat(p1.size()).isZero();
        assertThat(p1.toByteArray()).isEmpty();
        assertThat(p1).isSameAs(p2);
        assertThat(p1).isSameAs(p3);
        assertThat(p1).isSameAs(p4);
    }

    @Test
    void byteArrayPayload_shouldProvideAccessAndDefensiveClone() throws IOException {
        byte[] bytes = "hello world".getBytes(StandardCharsets.UTF_8);
        Payload payload = Payload.of(bytes);

        assertThat(payload.size()).isEqualTo(11);
        assertThat(payload.isEmpty()).isFalse();
        assertThat(payload.toByteArray()).isEqualTo(bytes);

        // Mutating source array does not affect payload
        bytes[0] = 'X';
        assertThat(new String(payload.toByteArray(), StandardCharsets.UTF_8)).isEqualTo("hello world");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        payload.writeTo(out);
        assertThat(out.toString(StandardCharsets.UTF_8)).isEqualTo("hello world");

        ByteBuffer buf = ByteBuffer.allocate(11);
        payload.writeTo(buf);
        assertThat(new String(buf.array(), StandardCharsets.UTF_8)).isEqualTo("hello world");

        ByteBuffer readOnly = payload.asReadOnlyByteBuffer();
        assertThat(readOnly.isReadOnly()).isTrue();
        assertThat(readOnly.remaining()).isEqualTo(11);
    }

    @Test
    void byteArrayPayload_fromByteBuffer() {
        ByteBuffer src = ByteBuffer.wrap("buffer payload".getBytes(StandardCharsets.UTF_8));
        Payload payload = Payload.of(src);

        assertThat(payload.size()).isEqualTo(14);
        assertThat(new String(payload.toByteArray(), StandardCharsets.UTF_8)).isEqualTo("buffer payload");
    }

    @Test
    void byteArrayPayload_wrapDoesNotCloneInitially() {
        byte[] bytes = "mutable".getBytes(StandardCharsets.UTF_8);
        Payload payload = Payload.wrap(bytes);

        bytes[0] = 'M';
        assertThat(new String(payload.toByteArray(), StandardCharsets.UTF_8)).isEqualTo("Mutable");
    }

    @Test
    void heapPayload_defaultLifecycleIsNoOp() {
        Payload payload = Payload.of("test".getBytes(StandardCharsets.UTF_8));

        assertThat(payload.isReferenceCounted()).isFalse();
        assertThat(payload.refCnt()).isEqualTo(1);
        assertThat(payload.retain()).isSameAs(payload);
        assertThat(payload.release()).isFalse();
        assertThat(payload.refCnt()).isEqualTo(1);
    }

    @Test
    void equalityBetweenByteArrayPayloads() {
        byte[] raw = "test-payload".getBytes(StandardCharsets.UTF_8);
        Payload p1 = Payload.of(raw);
        Payload p2 = Payload.wrap(raw.clone());

        assertThat(p1).isEqualTo(p2);
        assertThat(p2).isEqualTo(p1);
        assertThat(p1.hashCode()).isEqualTo(p2.hashCode());
    }
}
