package com.volo.broker.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class NettyDirectPayloadTest {

    @Test
    void directPayload_shouldDelegateRefCountingAndTriggerZeroRefCallback() {
        ByteBuf buf = Unpooled.directBuffer(16);
        buf.writeBytes("direct-data".getBytes(StandardCharsets.UTF_8));
        AtomicBoolean zeroRefInvoked = new AtomicBoolean(false);

        NettyDirectPayload payload = new NettyDirectPayload(buf, () -> zeroRefInvoked.set(true));

        assertThat(payload.isReferenceCounted()).isTrue();
        assertThat(payload.refCnt()).isEqualTo(1);
        assertThat(payload.size()).isEqualTo(11);

        payload.retain();
        assertThat(payload.refCnt()).isEqualTo(2);

        boolean released1 = payload.release();
        assertThat(released1).isFalse();
        assertThat(zeroRefInvoked.get()).isFalse();

        boolean released2 = payload.release();
        assertThat(released2).isTrue();
        assertThat(zeroRefInvoked.get()).isTrue();
    }

    @Test
    void directPayload_shouldReadAndWriteCorrectly() throws IOException {
        byte[] raw = "payload-content-test".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Unpooled.directBuffer(raw.length);
        buf.writeBytes(raw);

        NettyDirectPayload payload = new NettyDirectPayload(buf);

        assertThat(payload.toByteArray()).isEqualTo(raw);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        payload.writeTo(out);
        assertThat(out.toByteArray()).isEqualTo(raw);

        ByteBuffer dst = ByteBuffer.allocate(raw.length);
        payload.writeTo(dst);
        assertThat(dst.array()).isEqualTo(raw);

        payload.release();
    }

    @Test
    void equalityWithByteArrayPayload() {
        byte[] raw = "symmetric-payload".getBytes(StandardCharsets.UTF_8);
        ByteBuf buf = Unpooled.directBuffer(raw.length);
        buf.writeBytes(raw);

        NettyDirectPayload directPayload = new NettyDirectPayload(buf);
        ByteArrayPayload heapPayload = new ByteArrayPayload(raw);

        assertThat(directPayload).isEqualTo(heapPayload);
        assertThat(heapPayload).isEqualTo(directPayload);
        assertThat(directPayload.hashCode()).isEqualTo(heapPayload.hashCode());

        directPayload.release();
    }
}
