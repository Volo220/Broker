package com.volo.broker.transport.grpc.service;

import com.custombroker.grpc.PublishNack;
import com.custombroker.grpc.PublisherResponse;
import com.custombroker.grpc.PublisherServiceGrpc;
import com.volo.broker.broker.command.CommandBus;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.error.BrokerException;
import com.volo.broker.broker.flow.QueueFlowController;
import com.volo.broker.buffer.HeapPayloadAllocator;
import com.volo.broker.config.GrpcPublisherProperties;
import com.volo.broker.transport.grpc.SafeGrpcObserver;
import com.volo.broker.transport.grpc.codec.inbound.PublisherFrame;
import com.volo.broker.transport.grpc.codec.inbound.PublisherRequestMarshaller;
import com.volo.broker.transport.grpc.connection.PublisherConnection;
import com.volo.broker.transport.grpc.connection.PublisherConnectionRegistry;
import com.volo.broker.transport.grpc.flow.PublisherFlowGate;
import com.volo.broker.transport.grpc.mapper.GrpcErrorMapper;
import com.volo.broker.transport.grpc.mapper.GrpcMessageConverters;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
public class GrpcPublisherService implements BindableService {
    private static final Logger log = LoggerFactory.getLogger(GrpcPublisherService.class);

    private final CommandBus commandBus;
    private final PublisherConnectionRegistry registry;
    private final QueueFlowController flowController;
    private final PublisherRequestMarshaller requestMarshaller;

    public GrpcPublisherService(CommandBus commandBus, PublisherConnectionRegistry registry) {
        this(commandBus, registry, null, new PublisherRequestMarshaller(new HeapPayloadAllocator(), GrpcPublisherProperties.defaults()));
    }

    public GrpcPublisherService(
            CommandBus commandBus,
            PublisherConnectionRegistry registry,
            PublisherRequestMarshaller requestMarshaller
    ) {
        this(commandBus, registry, null, requestMarshaller);
    }

    @Autowired
    public GrpcPublisherService(
            CommandBus commandBus,
            PublisherConnectionRegistry registry,
            @Nullable QueueFlowController flowController,
            PublisherRequestMarshaller requestMarshaller
    ) {
        this.commandBus = Objects.requireNonNull(commandBus, "commandBus must not be null");
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.flowController = flowController;
        this.requestMarshaller = Objects.requireNonNull(requestMarshaller, "requestMarshaller must not be null");
    }

    @Override
    public ServerServiceDefinition bindService() {
        MethodDescriptor<PublisherFrame, PublisherResponse> method = PublisherServiceGrpc
                .getOpenPublisherChannelMethod()
                .toBuilder(requestMarshaller, PublisherServiceGrpc.getOpenPublisherChannelMethod().getResponseMarshaller())
                .build();

        return ServerServiceDefinition.builder(PublisherServiceGrpc.SERVICE_NAME)
                .addMethod(method, ServerCalls.asyncBidiStreamingCall(this::openPublisherChannel))
                .build();
    }

    public StreamObserver<PublisherFrame> openPublisherChannel(StreamObserver<PublisherResponse> responseObserver) {
        ServerCallStreamObserver<PublisherResponse> serverCall = responseObserver instanceof ServerCallStreamObserver<PublisherResponse> sco
                ? sco
                : null;
        if (serverCall != null) {
            serverCall.disableAutoRequest();
        }

        PublisherConnection connection = new PublisherConnection(new SafeGrpcObserver<>(responseObserver));
        PublisherFlowGate gate = new PublisherFlowGate(serverCall, flowController, connection);
        connection.attachFlowGate(gate);
        registry.register(connection);

        if (serverCall != null) {
            serverCall.request(1);
        }

        return createStreamObserver(connection, gate);
    }

    private StreamObserver<PublisherFrame> createStreamObserver(PublisherConnection connection, PublisherFlowGate gate) {
        return new StreamObserver<>() {
            @Override
            public void onNext(PublisherFrame frame) {
                processRequestSafely(connection, gate, frame);
            }

            @Override
            public void onError(Throwable throwable) {
                closeConnection(connection);
            }

            @Override
            public void onCompleted() {
                closeConnection(connection);
            }
        };
    }

    private void processRequestSafely(PublisherConnection conn, PublisherFlowGate gate, PublisherFrame frame) {
        if (conn.isClosed()) {
            if (frame != null) frame.release();
            return;
        }
        conn.markSeen();
        List<String> routedQueues = List.of();
        try {
            routedQueues = handleFrame(conn, frame);
        } catch (BrokerException exception) {
            sendNackResponse(conn, getClientPublishId(frame), GrpcErrorMapper.toProto(exception));
        } catch (Throwable throwable) {
            log.error("Unexpected publisher request error", throwable);
            sendNackResponse(conn, getClientPublishId(frame), GrpcErrorMapper.internalError(throwable));
        } finally {
            if (frame != null) frame.release();
            gate.afterRequestProcessed(routedQueues);
        }
    }

    private void sendNackResponse(PublisherConnection conn, Long publishId, com.custombroker.grpc.ErrorInfo error) {
        conn.send(PublisherResponse.newBuilder()
                .setNack(PublishNack.newBuilder()
                        .setClientPublishId(publishId)
                        .setError(error)
                        .build())
                .build());
    }

    private List<String> handleFrame(PublisherConnection connection, PublisherFrame frame) {
        if (frame == null) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "Publisher frame must not be null");
        }
        if (frame instanceof PublisherFrame.Invalid invalid) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, invalid.reason());
        }
        if (frame instanceof PublisherFrame.Ping) {
            connection.send(GrpcMessageConverters.toPongResponse());
            return List.of();
        }
        if (frame instanceof PublisherFrame.Close) {
            closeConnection(connection);
            return List.of();
        }
        if (frame instanceof PublisherFrame.Publish pub) {
            return handlePublish(connection, pub);
        }
        throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "Unknown publisher frame");
    }

    private List<String> handlePublish(PublisherConnection connection, PublisherFrame.Publish pub) {
        long clientPublishId = pub.clientPublishId();
        if (pub.rejectCode() != null) {
            PublishResult result = PublishResult.nack(clientPublishId, pub.rejectCode(), pub.rejectReason());
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return List.of();
        }

        if (connection.flowGate() != null && connection.flowGate().isOverdue()) {
            connection.flowGate().clearOverdue();
            if (flowController != null) {
                flowController.recordOverdueRejection();
            }
            PublishResult result = PublishResult.nack(
                    clientPublishId,
                    BrokerErrorCode.RESOURCE_EXHAUSTED,
                    "Flow control timeout exceeded: downstream queue is blocked"
            );
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return List.of();
        }

        PublishCommand command = new PublishCommand(clientPublishId, pub.routingKey(), pub.payload(), pub.headers());
        try {
            PublishResult result = commandBus.execute(command);
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return result.routedQueuePaths();
        } catch (BrokerException exception) {
            PublishResult result = PublishResult.nack(clientPublishId, exception.code(), exception.getMessage());
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return List.of();
        } catch (Throwable throwable) {
            log.error("Publish failed", throwable);
            PublishResult result = PublishResult.nack(clientPublishId, BrokerErrorCode.INTERNAL_ERROR, "Message was not accepted by broker");
            connection.send(GrpcMessageConverters.toPublisherResponse(result));
            return List.of();
        }
    }

    private void closeConnection(PublisherConnection connection) {
        registry.unregister(connection.connectionId());
        connection.close();
    }

    private static Long getClientPublishId(PublisherFrame frame) {
        if (frame instanceof PublisherFrame.Publish pub) {
            return pub.clientPublishId();
        }
        return 0L;
    }
}
