package com.volo.broker.broker.message;

import com.google.protobuf.ByteString;
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
        Payload p3 = Payload.of(ByteString.EMPTY);

        assertThat(p1.isEmpty()).isTrue();
        assertThat(p1.size()).isZero();
        assertThat(p1.toByteArray()).isEmpty();
        assertThat(p1.toByteString().isEmpty()).isTrue();
        assertThat(p1).isSameAs(p2);
        assertThat(p1).isSameAs(p3);
    }

    @Test
    void byteStringPayload_shouldProvideZeroCopyAccess() throws IOException {
        ByteString bs = ByteString.copyFromUtf8("hello world");
        Payload payload = Payload.of(bs);

        assertThat(payload.size()).isEqualTo(11);
        assertThat(payload.isEmpty()).isFalse();
        assertThat(payload.toByteString()).isSameAs(bs);
        assertThat(payload.toByteArray()).isEqualTo("hello world".getBytes(StandardCharsets.UTF_8));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        payload.writeTo(out);
        assertThat(out.toString(StandardCharsets.UTF_8)).isEqualTo("hello world");

        ByteBuffer buf = ByteBuffer.allocate(11);
        payload.writeTo(buf);
        assertThat(new String(buf.array(), StandardCharsets.UTF_8)).isEqualTo("hello world");
    }

    @Test
    void byteArrayPayload_shouldDefensivelyCloneOnExternalMutation() {
        byte[] bytes = "mutable".getBytes(StandardCharsets.UTF_8);
        Payload payload = Payload.of(bytes);

        bytes[0] = 'X';

        assertThat(new String(payload.toByteArray(), StandardCharsets.UTF_8)).isEqualTo("mutable");
    }

    @Test
    void equalityBetweenImplementations() {
        byte[] raw = "test-payload".getBytes(StandardCharsets.UTF_8);
        Payload p1 = Payload.of(raw);
        Payload p2 = Payload.of(ByteString.copyFrom(raw));

        assertThat(p1).isEqualTo(p2);
        assertThat(p2).isEqualTo(p1);
        assertThat(p1.hashCode()).isEqualTo(p2.hashCode());
    }
}

