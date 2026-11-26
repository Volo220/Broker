package com.volo.broker.transport.grpc.codec.inbound;

import com.custombroker.grpc.ClosePublisherAction;
import com.custombroker.grpc.KeepAlive;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.buffer.Payload;
import com.volo.broker.buffer.PayloadAllocator;
import com.volo.broker.config.GrpcPublisherProperties;
import io.grpc.MethodDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * gRPC inbound request marshaller for {@link PublisherFrame}.
 * Orchestrates top-level request frame routing and delegates to specialized parser and validator.
 */
@Component
public class PublisherRequestMarshaller implements MethodDescriptor.Marshaller<PublisherFrame> {

    private final PublishActionParser publishActionParser;
    private final PublishFrameValidator validator;

    @Autowired
    public PublisherRequestMarshaller(PublishActionParser publishActionParser, PublishFrameValidator validator) {
        this.publishActionParser = Objects.requireNonNull(publishActionParser, "publishActionParser must not be null");
        this.validator = Objects.requireNonNull(validator, "validator must not be null");
    }

    public PublisherRequestMarshaller(PayloadAllocator payloadAllocator, GrpcPublisherProperties properties) {
        this(new PublishActionParser(payloadAllocator, properties), new PublishFrameValidator(properties));
    }

    @Override
    public InputStream stream(PublisherFrame value) {
        throw new UnsupportedOperationException("Server-side request marshaller only supports inbound parsing");
    }

    @Override
    public PublisherFrame parse(InputStream stream) {
        if (stream == null) {
            return new PublisherFrame.Invalid("Stream must not be null");
        }
        ProtobufWireReader reader = new ProtobufWireReader(stream);
        try {
            int field;
            while ((field = reader.nextFieldNumber()) != 0) {
                if (field == 1) return parseAndValidatePublish(reader);
                if (field == 2) return parsePing(reader);
                if (field == 3) return parseClose(reader);
                reader.skipCurrentField();
            }
            return new PublisherFrame.Invalid("Publisher request must contain one request type");
        } catch (Throwable t) {
            return new PublisherFrame.Invalid("Failed to parse publisher request: " + t.getMessage());
        }
    }

    private PublisherFrame parseAndValidatePublish(ProtobufWireReader reader) throws IOException {
        reader.readVarint32();
        PublisherFrame frame = publishActionParser.parse(reader);
        if (!(frame instanceof PublisherFrame.Publish pub)) {
            return frame;
        }
        if (pub.rejectCode() != null) {
            return pub;
        }
        String error = validator.validate(pub.routingKey(), pub.payload(), pub.headers());
        if (error != null) {
            pub.release();
            return new PublisherFrame.Publish(
                    pub.clientPublishId(),
                    pub.routingKey(),
                    Payload.empty(),
                    pub.headers(),
                    BrokerErrorCode.INVALID_REQUEST,
                    error
            );
        }
        return pub;
    }

    private PublisherFrame parsePing(ProtobufWireReader reader) throws IOException {
        int len = reader.readVarint32();
        byte[] bytes = reader.readBytes(len);
        return new PublisherFrame.Ping(KeepAlive.parseFrom(bytes));
    }

    private PublisherFrame parseClose(ProtobufWireReader reader) throws IOException {
        int len = reader.readVarint32();
        byte[] bytes = reader.readBytes(len);
        return new PublisherFrame.Close(ClosePublisherAction.parseFrom(bytes));
    }
}
