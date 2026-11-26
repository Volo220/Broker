package com.volo.broker.transport.grpc.codec.inbound;

import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.buffer.Payload;
import com.volo.broker.buffer.PayloadAllocator;
import com.volo.broker.config.GrpcPublisherProperties;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Parses binary wire format of {@link com.custombroker.grpc.PublishAction} into a {@link PublisherFrame}.
 */
@Component
public class PublishActionParser {

    private final PayloadAllocator payloadAllocator;
    private final GrpcPublisherProperties properties;

    public PublishActionParser(PayloadAllocator payloadAllocator, GrpcPublisherProperties properties) {
        this.payloadAllocator = Objects.requireNonNull(payloadAllocator, "payloadAllocator must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    public PublisherFrame parse(ProtobufWireReader reader) {
        long clientPublishId = 0L;
        String routingKey = "";
        Payload payload = null;
        Map<String, String> headers = new HashMap<>();
        BrokerErrorCode rejectCode = null;
        String rejectReason = null;

        try {
            int field;
            while ((field = reader.nextFieldNumber()) != 0) {
                switch (field) {
                    case 1 -> clientPublishId = reader.readVarint64();
                    case 2 -> routingKey = reader.readString(reader.readVarint32());
                    case 3 -> {
                        int len = reader.readVarint32();
                        if (len > properties.maxPayloadBytes()) {
                            reader.skip(len);
                            rejectCode = BrokerErrorCode.PAYLOAD_TOO_LARGE;
                            rejectReason = "payload size exceeds broker limit";
                        } else if (len > 0) {
                            payload = payloadAllocator.allocateFrom(reader.inputStream(), len);
                        }
                    }
                    case 4 -> parseHeaderEntry(reader, headers);
                    default -> reader.skipCurrentField();
                }
            }
            Payload finalPayload = payload == null ? Payload.empty() : payload;
            return new PublisherFrame.Publish(clientPublishId, routingKey, finalPayload, headers, rejectCode, rejectReason);
        } catch (Throwable t) {
            if (payload != null) {
                payload.release();
            }
            return new PublisherFrame.Invalid("Failed to parse publish request: " + t.getMessage());
        }
    }

    private void parseHeaderEntry(ProtobufWireReader reader, Map<String, String> headers) throws IOException {
        int entryLen = reader.readVarint32();
        if (entryLen <= 0) {
            return;
        }
        byte[] bytes = reader.readBytes(entryLen);
        ProtobufWireReader entryReader = new ProtobufWireReader(new ByteArrayInputStream(bytes));
        String key = "";
        String value = "";
        int field;
        while ((field = entryReader.nextFieldNumber()) != 0) {
            if (field == 1) {
                key = entryReader.readString(entryReader.readVarint32());
            } else if (field == 2) {
                value = entryReader.readString(entryReader.readVarint32());
            } else {
                entryReader.skipCurrentField();
            }
        }
        headers.put(key, value);
    }
}
