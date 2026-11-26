package com.volo.broker.transport.grpc.codec.inbound;

import com.volo.broker.buffer.Payload;
import com.volo.broker.config.GrpcPublisherProperties;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

/**
 * Validates inbound publish action parameters against configured limits.
 */
@Component
public class PublishFrameValidator {

    private final GrpcPublisherProperties properties;

    public PublishFrameValidator(GrpcPublisherProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    /**
     * Validates publish action fields. Returns an error message if invalid, or {@code null} if valid.
     */
    public @Nullable String validate(
            @Nullable String routingKey,
            @Nullable Payload payload,
            @Nullable Map<String, String> headers
    ) {
        if (routingKey == null || routingKey.isBlank()) {
            return "routingKey is required";
        }
        if (routingKey.length() > properties.maxRoutingKeyLength()) {
            return "routingKey is too long";
        }
        if (payload == null || payload.isEmpty()) {
            return "payload must not be empty";
        }
        if (headers != null && headers.size() > properties.maxHeaders()) {
            return "too many headers";
        }
        return headers == null ? null : validateHeaders(headers);
    }

    private @Nullable String validateHeaders(Map<String, String> headers) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            if (key == null || key.isBlank()) {
                return "header key must not be blank";
            }
            if (key.length() > properties.maxHeaderKeyLength()) {
                return "header key is too long";
            }
            if (val != null && val.length() > properties.maxHeaderValueLength()) {
                return "header value is too long";
            }
        }
        return null;
    }
}
