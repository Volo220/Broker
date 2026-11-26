package com.volo.broker.transport.grpc.service;

import com.custombroker.grpc.ClosePublisherAction;
import com.custombroker.grpc.ErrorCode;
import com.custombroker.grpc.KeepAlive;
import com.custombroker.grpc.PublisherResponse;
import com.volo.broker.TestStreamObserver;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.buffer.Payload;
import com.volo.broker.transport.grpc.codec.inbound.PublisherFrame;
import com.volo.broker.transport.grpc.connection.PublisherConnectionRegistry;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class GrpcPublisherServiceTest {

    private final CommandBus commandBus = mock(CommandBus.class);
    private final PublisherConnectionRegistry connectionRegistry = new PublisherConnectionRegistry();

    private final GrpcPublisherService service = new GrpcPublisherService(
            commandBus,
            connectionRegistry
    );

    @Test
    @DisplayName("openPublisherChannel sends Ack when publish accepted")
    void openPublisherChannel_shouldSendAckWhenPublishAccepted() {
        when(commandBus.execute(any(PublishCommand.class)))
                .thenReturn(PublishResult.ack(1L, 1));

        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();
        StreamObserver<PublisherFrame> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(new PublisherFrame.Publish(
                1L,
                "orders.created",
                Payload.of("payload".getBytes(StandardCharsets.UTF_8)),
                Map.of(),
                null,
                null
        ));

        assertThat(responseObserver.values()).hasSize(1);
        PublisherResponse response = responseObserver.singleValue();
        assertThat(response.hasAck()).isTrue();
        assertThat(response.getAck().getClientPublishId()).isEqualTo(1L);
        assertThat(response.getAck().getRoutedQueueCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("openPublisherChannel sends Nack when domain rejects publish")
    void openPublisherChannel_shouldSendNackWhenPublishRejected() {
        when(commandBus.execute(any(PublishCommand.class)))
                .thenReturn(PublishResult.nack(
                        1L,
                        BrokerErrorCode.QUEUE_NOT_FOUND,
                        "No route"
                ));

        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();
        StreamObserver<PublisherFrame> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(new PublisherFrame.Publish(
                1L,
                "missing.route",
                Payload.of("payload".getBytes(StandardCharsets.UTF_8)),
                Map.of(),
                null,
                null
        ));

        PublisherResponse response = responseObserver.singleValue();
        assertThat(response.hasNack()).isTrue();
        assertThat(response.getNack().getClientPublishId()).isEqualTo(1L);
        assertThat(response.getNack().getError().getCode()).isEqualTo(ErrorCode.QUEUE_NOT_FOUND);
    }

    @Test
    @DisplayName("openPublisherChannel sends Nack directly when frame has rejectCode")
    void openPublisherChannel_shouldSendNackWhenFrameAlreadyRejected() {
        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();
        StreamObserver<PublisherFrame> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(new PublisherFrame.Publish(
                2L,
                "route",
                Payload.empty(),
                Map.of(),
                BrokerErrorCode.PAYLOAD_TOO_LARGE,
                "payload size exceeds broker limit"
        ));

        verifyNoInteractions(commandBus);
        PublisherResponse response = responseObserver.singleValue();
        assertThat(response.hasNack()).isTrue();
        assertThat(response.getNack().getClientPublishId()).isEqualTo(2L);
        assertThat(response.getNack().getError().getCode()).isEqualTo(ErrorCode.PAYLOAD_TOO_LARGE);
    }

    @Test
    @DisplayName("openPublisherChannel sends Nack on Invalid frame")
    void openPublisherChannel_shouldSendNackOnInvalidFrame() {
        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();
        StreamObserver<PublisherFrame> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(new PublisherFrame.Invalid("Corrupted frame"));

        PublisherResponse response = responseObserver.singleValue();
        assertThat(response.hasNack()).isTrue();
        assertThat(response.getNack().getError().getCode()).isEqualTo(ErrorCode.INVALID_REQUEST);
        assertThat(response.getNack().getError().getMessage()).contains("Corrupted frame");
    }

    @Test
    @DisplayName("openPublisherChannel responds with Pong on Ping frame")
    void openPublisherChannel_shouldRespondWithPongOnPing() {
        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();
        StreamObserver<PublisherFrame> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(new PublisherFrame.Ping(
                KeepAlive.newBuilder().setTimestamp(123L).build()
        ));

        PublisherResponse response = responseObserver.singleValue();
        assertThat(response.hasPong()).isTrue();
        assertThat(response.getPong().getTimestamp()).isGreaterThanOrEqualTo(123L);
    }

    @Test
    @DisplayName("openPublisherChannel closes connection on Close frame")
    void openPublisherChannel_shouldCompleteOnCloseAction() {
        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();
        StreamObserver<PublisherFrame> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(new PublisherFrame.Close(
                ClosePublisherAction.newBuilder().setReason("client-close").build()
        ));

        assertThat(responseObserver.completed()).isTrue();
    }

    @Test
    @DisplayName("openPublisherChannel guarantees payload release in finally")
    void openPublisherChannel_shouldReleasePayloadInFinally() {
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

        when(commandBus.execute(any(PublishCommand.class)))
                .thenReturn(PublishResult.ack(5L, 1));

        TestStreamObserver<PublisherResponse> responseObserver = new TestStreamObserver<>();
        StreamObserver<PublisherFrame> requestObserver =
                service.openPublisherChannel(responseObserver);

        requestObserver.onNext(new PublisherFrame.Publish(
                5L,
                "orders.created",
                trackingPayload,
                Map.of(),
                null,
                null
        ));

        assertThat(released.get()).isTrue();
    }
}
