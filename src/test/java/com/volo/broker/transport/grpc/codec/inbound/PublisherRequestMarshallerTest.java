package com.volo.broker.transport.grpc.codec.inbound;

import com.custombroker.grpc.*;
import com.google.protobuf.ByteString;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.buffer.HeapPayloadAllocator;
import com.volo.broker.buffer.Payload;
import com.volo.broker.buffer.PayloadAllocator;
import com.volo.broker.config.GrpcPublisherProperties;
import io.grpc.HasByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class PublisherRequestMarshallerTest {

    private GrpcPublisherProperties properties;
    private PayloadAllocator payloadAllocator;
    private PublisherRequestMarshaller marshaller;

    @BeforeEach
    void setUp() {
        properties = new GrpcPublisherProperties(
                1024, // maxPayloadBytes: 1KB for testing
                64,   // maxRoutingKeyLength
                5,    // maxHeaders
                32,   // maxHeaderKeyLength
                64    // maxHeaderValueLength
        );
        payloadAllocator = new HeapPayloadAllocator();
        marshaller = new PublisherRequestMarshaller(payloadAllocator, properties);
    }

    private InputStream toStream(PublisherRequest request) {
        return new ByteArrayInputStream(request.toByteArray());
    }

    @Nested
    @DisplayName("Happy Path Parsing")
    class HappyPathTests {

        @Test
        @DisplayName("Parses standard PublishAction into PublisherFrame.Publish")
        void shouldParsePublishAction() {
            byte[] payloadBytes = "Hello gRPC payload".getBytes(StandardCharsets.UTF_8);
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(PublishAction.newBuilder()
                            .setClientPublishId(101L)
                            .setRoutingKey("orders.create")
                            .setPayload(ByteString.copyFrom(payloadBytes))
                            .putHeaders("traceId", "abc-123")
                            .putHeaders("region", "eu-west")
                            .build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
            PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
            assertThat(pub.clientPublishId()).isEqualTo(101L);
            assertThat(pub.routingKey()).isEqualTo("orders.create");
            assertThat(pub.payload().toByteArray()).isEqualTo(payloadBytes);
            assertThat(pub.headers()).containsEntry("traceId", "abc-123").containsEntry("region", "eu-west");
            assertThat(pub.rejectCode()).isNull();
            assertThat(pub.rejectReason()).isNull();
        }

        @Test
        @DisplayName("Parses KeepAlive into PublisherFrame.Ping")
        void shouldParseKeepAlive() {
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPing(KeepAlive.newBuilder()
                            .setTimestamp(99998888L)
                            .build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Ping.class);
            PublisherFrame.Ping ping = (PublisherFrame.Ping) frame;
            assertThat(ping.keepAlive().getTimestamp()).isEqualTo(99998888L);
        }

        @Test
        @DisplayName("Parses ClosePublisherAction into PublisherFrame.Close")
        void shouldParseClose() {
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setClose(ClosePublisherAction.newBuilder()
                            .setReason("client-shutting-down")
                            .build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Close.class);
            PublisherFrame.Close close = (PublisherFrame.Close) frame;
            assertThat(close.action().getReason()).isEqualTo("client-shutting-down");
        }
    }

    @Nested
    @DisplayName("Limit Enforcement & Safe Skip")
    class LimitEnforcementTests {

        @Test
        @DisplayName("Rejects payload exceeding maxPayloadBytes with PAYLOAD_TOO_LARGE and skips bytes")
        void shouldRejectAndSkipWhenPayloadExceedsLimit() {
            byte[] oversized = new byte[1025]; // limit is 1024
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(PublishAction.newBuilder()
                            .setClientPublishId(202L)
                            .setRoutingKey("large.message")
                            .setPayload(ByteString.copyFrom(oversized))
                            .build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
            PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
            assertThat(pub.clientPublishId()).isEqualTo(202L);
            assertThat(pub.rejectCode()).isEqualTo(BrokerErrorCode.PAYLOAD_TOO_LARGE);
            assertThat(pub.rejectReason()).contains("exceeds broker limit");
            assertThat(pub.payload()).isEqualTo(Payload.empty());
        }

        @Test
        @DisplayName("Rejects empty or missing payload with INVALID_REQUEST")
        void shouldRejectEmptyPayload() {
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(PublishAction.newBuilder()
                            .setClientPublishId(303L)
                            .setRoutingKey("test.route")
                            .build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
            PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
            assertThat(pub.rejectCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
            assertThat(pub.rejectReason()).contains("payload must not be empty");
        }

        @Test
        @DisplayName("Rejects blank or missing routingKey with INVALID_REQUEST")
        void shouldRejectBlankRoutingKey() {
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(PublishAction.newBuilder()
                            .setClientPublishId(404L)
                            .setRoutingKey("   ")
                            .setPayload(ByteString.copyFromUtf8("valid payload"))
                            .build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
            PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
            assertThat(pub.rejectCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
            assertThat(pub.rejectReason()).contains("routingKey is required");
        }

        @Test
        @DisplayName("Rejects routingKey exceeding maxRoutingKeyLength")
        void shouldRejectOversizedRoutingKey() {
            String longKey = "a".repeat(65); // limit is 64
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(PublishAction.newBuilder()
                            .setClientPublishId(505L)
                            .setRoutingKey(longKey)
                            .setPayload(ByteString.copyFromUtf8("valid payload"))
                            .build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
            PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
            assertThat(pub.rejectCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
            assertThat(pub.rejectReason()).contains("routingKey is too long");
        }

        @Test
        @DisplayName("Rejects headers exceeding maxHeaders")
        void shouldRejectTooManyHeaders() {
            PublishAction.Builder actionBuilder = PublishAction.newBuilder()
                    .setClientPublishId(606L)
                    .setRoutingKey("route")
                    .setPayload(ByteString.copyFromUtf8("valid payload"));

            for (int i = 0; i < 6; i++) { // limit is 5
                actionBuilder.putHeaders("k" + i, "v" + i);
            }

            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(actionBuilder.build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
            PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
            assertThat(pub.rejectCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
            assertThat(pub.rejectReason()).contains("too many headers");
        }

        @Test
        @DisplayName("Rejects header key that is blank or too long")
        void shouldRejectInvalidHeaderKey() {
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(PublishAction.newBuilder()
                            .setClientPublishId(707L)
                            .setRoutingKey("route")
                            .setPayload(ByteString.copyFromUtf8("valid payload"))
                            .putHeaders("a".repeat(33), "value") // limit is 32
                            .build())
                    .build();

            PublisherFrame frame = marshaller.parse(toStream(request));

            assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
            PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
            assertThat(pub.rejectCode()).isEqualTo(BrokerErrorCode.INVALID_REQUEST);
            assertThat(pub.rejectReason()).contains("header key is too long");
        }
    }

    @Nested
    @DisplayName("Error & Resource Safety")
    class SafetyTests {

        @Test
        @DisplayName("Returns Invalid frame on empty stream")
        void shouldReturnInvalidOnEmptyStream() {
            PublisherFrame frame = marshaller.parse(new ByteArrayInputStream(new byte[0]));
            assertThat(frame).isInstanceOf(PublisherFrame.Invalid.class);
        }

        @Test
        @DisplayName("Releases allocated payload if stream is truncated during subsequent fields")
        void shouldReleasePayloadOnPrematureEof() {
            AtomicBoolean released = new AtomicBoolean(false);
            Payload mockPayload = new Payload() {
                @Override
                public int size() {
                    return 10;
                }

                @Override
                public byte[] toByteArray() {
                    return new byte[10];
                }

                @Override
                public ByteBuffer asReadOnlyByteBuffer() {
                    return ByteBuffer.wrap(new byte[10]);
                }

                @Override
                public void writeTo(java.io.OutputStream out) {
                }

                @Override
                public void writeTo(ByteBuffer buffer) {
                }

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
                    return mockPayload;
                }

                @Override
                public Payload allocate(int size, PayloadWriter writer) {
                    return mockPayload;
                }

                @Override
                public long usedDirectBytes() {
                    return 0;
                }

                @Override
                public long liveBuffers() {
                    return 0;
                }
            };

            PublisherRequestMarshaller customMarshaller =
                    new PublisherRequestMarshaller(trackingAllocator, properties);

            // Create a valid request, serialize it, and truncate the last few bytes (cutting into headers)
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(PublishAction.newBuilder()
                            .setClientPublishId(808L)
                            .setRoutingKey("route")
                            .setPayload(ByteString.copyFromUtf8("some bytes"))
                            .putHeaders("traceId", "very-long-header-value-that-will-be-cut-off")
                            .build())
                    .build();

            byte[] fullBytes = request.toByteArray();
            byte[] truncated = new byte[fullBytes.length - 10];
            System.arraycopy(fullBytes, 0, truncated, 0, truncated.length);

            PublisherFrame frame = customMarshaller.parse(new ByteArrayInputStream(truncated));

            assertThat(frame).isInstanceOf(PublisherFrame.Invalid.class);
            assertThat(released.get()).isTrue();
        }

        @Test
        @DisplayName("Supports HasByteBuffer stream directly")
        void shouldSupportHasByteBufferStream() {
            PublisherRequest request = PublisherRequest.newBuilder()
                    .setPublish(PublishAction.newBuilder()
                            .setClientPublishId(909L)
                            .setRoutingKey("has.byte.buffer")
                            .setPayload(ByteString.copyFromUtf8("hello direct"))
                            .build())
                    .build();

            byte[] bytes = request.toByteArray();
            TestHasByteBufferStream stream = new TestHasByteBufferStream(bytes);

            PublisherFrame frame = marshaller.parse(stream);
            assertThat(frame).isInstanceOf(PublisherFrame.Publish.class);
            PublisherFrame.Publish pub = (PublisherFrame.Publish) frame;
            assertThat(pub.payload().toByteArray()).isEqualTo("hello direct".getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class TestHasByteBufferStream extends ByteArrayInputStream implements HasByteBuffer {
        public TestHasByteBufferStream(byte[] buf) {
            super(buf);
        }

        @Override
        public boolean byteBufferSupported() {
            return true;
        }

        @Override
        public ByteBuffer getByteBuffer() {
            return ByteBuffer.wrap(buf, pos, count - pos).asReadOnlyBuffer();
        }
    }
}
