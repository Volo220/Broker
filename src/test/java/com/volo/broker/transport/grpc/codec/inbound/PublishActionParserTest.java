package com.volo.broker.transport.grpc.codec.inbound;

import com.custombroker.grpc.PublishAction;
import com.google.protobuf.ByteString;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.buffer.HeapPayloadAllocator;
import com.volo.broker.buffer.Payload;
import com.volo.broker.buffer.PayloadAllocator;
import com.volo.broker.config.GrpcPublisherProperties;
import io.grpc.HasByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class PublishActionParserTest {

    private GrpcPublisherProperties properties;
    private PayloadAllocator payloadAllocator;
    private PublishActionParser parser;

    @BeforeEach
    void setUp() {
        properties = new GrpcPublisherProperties(1024, 64, 5, 32, 64);
        payloadAllocator = new HeapPayloadAllocator();
        parser = new PublishActionParser(payloadAllocator, properties);
    }

    private ProtobufWireReader toReader(PublishAction action) {
        return new ProtobufWireReader(new ByteArrayInputStream(action.toByteArray()));
    }

    @Test
    @DisplayName("Parses valid PublishAction correctly")
    void parse_shouldParseValidAction() {
        byte[] payloadBytes = "test-payload".getBytes(StandardCharsets.UTF_8);
        PublishAction action = PublishAction.newBuilder()
                .setClientPublishId(101L)
                .setRoutingKey("orders.created")
                .setPayload(ByteString.copyFrom(payloadBytes))
                .putHeaders("traceId", "tx-999")
                .build();

        PublisherFrame frame = parser.parse(toReader(action));

        assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
        PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
        assertThat(pub.clientPublishId()).isEqualTo(101L);
        assertThat(pub.routingKey()).isEqualTo("orders.created");
        assertThat(pub.payload().toByteArray()).isEqualTo(payloadBytes);
        assertThat(pub.headers()).containsEntry("traceId", "tx-999");
        assertThat(pub.rejectCode()).isNull();
    }

    @Test
    @DisplayName("Rejects payload exceeding maxPayloadBytes and safely skips stream")
    void parse_shouldRejectOversizedPayload() {
        byte[] oversized = new byte[1025];
        PublishAction action = PublishAction.newBuilder()
                .setClientPublishId(202L)
                .setRoutingKey("oversized")
                .setPayload(ByteString.copyFrom(oversized))
                .build();

        PublisherFrame frame = parser.parse(toReader(action));

        assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
        PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
        assertThat(pub.clientPublishId()).isEqualTo(202L);
        assertThat(pub.rejectCode()).isEqualTo(BrokerErrorCode.PAYLOAD_TOO_LARGE);
        assertThat(pub.rejectReason()).contains("exceeds broker limit");
        assertThat(pub.payload()).isEqualTo(Payload.empty());
    }

    @Test
    @DisplayName("Releases allocated payload on premature EOF")
    void parse_shouldReleasePayloadOnPrematureEof() {
        AtomicBoolean released = new AtomicBoolean(false);
        Payload trackingPayload = new Payload() {
            @Override public int size() { return 10; }
            @Override public byte[] toByteArray() { return new byte[10]; }
            @Override public ByteBuffer asReadOnlyByteBuffer() { return ByteBuffer.wrap(new byte[10]); }
            @Override public void writeTo(java.io.OutputStream out) {}
            @Override public void writeTo(ByteBuffer buffer) {}
            @Override
            public boolean release() {
                released.set(true);
                return true;
            }
        };

        PayloadAllocator trackingAllocator = new PayloadAllocator() {
            @Override
            public Payload allocateFrom(InputStream in, int size) throws IOException {
                in.readNBytes(size);
                return trackingPayload;
            }

            @Override public Payload allocate(int size, PayloadWriter writer) { return trackingPayload; }
            @Override public long usedDirectBytes() { return 0; }
            @Override public long liveBuffers() { return 0; }
        };

        PublishActionParser customParser = new PublishActionParser(trackingAllocator, properties);

        PublishAction action = PublishAction.newBuilder()
                .setClientPublishId(303L)
                .setRoutingKey("route")
                .setPayload(ByteString.copyFromUtf8("allocated"))
                .putHeaders("h1", "very-long-header-truncated-before-end")
                .build();

        byte[] bytes = action.toByteArray();
        byte[] truncated = new byte[bytes.length - 8];
        System.arraycopy(bytes, 0, truncated, 0, truncated.length);

        PublisherFrame frame = customParser.parse(new ProtobufWireReader(new ByteArrayInputStream(truncated)));

        assertThat(frame).isInstanceOf(PublisherFrame.Invalid.class);
        assertThat(released.get()).isTrue();
    }

    @Test
    @DisplayName("Supports HasByteBuffer stream")
    void parse_shouldSupportHasByteBufferStream() {
        PublishAction action = PublishAction.newBuilder()
                .setClientPublishId(404L)
                .setRoutingKey("direct.buffer")
                .setPayload(ByteString.copyFromUtf8("direct payload"))
                .build();

        TestHasByteBufferStream stream = new TestHasByteBufferStream(action.toByteArray());
        PublisherFrame frame = parser.parse(new ProtobufWireReader(stream));

        assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
        PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
        assertThat(pub.payload().toByteArray()).isEqualTo("direct payload".getBytes(StandardCharsets.UTF_8));
    }

    private static class TestHasByteBufferStream extends ByteArrayInputStream implements HasByteBuffer {
        public TestHasByteBufferStream(byte[] buf) {
            super(buf);
        }

        @Override public boolean byteBufferSupported() { return true; }
        @Override public ByteBuffer getByteBuffer() { return ByteBuffer.wrap(buf, pos, count - pos).asReadOnlyBuffer(); }
    }
}
