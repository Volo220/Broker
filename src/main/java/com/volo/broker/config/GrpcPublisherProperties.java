package com.volo.broker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration properties for incoming gRPC publisher requests and limits.
 */
@ConfigurationProperties(prefix = "broker.transport.grpc.publisher")
public record GrpcPublisherProperties(
        @DefaultValue("4194304") int maxPayloadBytes,
        @DefaultValue("255") int maxRoutingKeyLength,
        @DefaultValue("64") int maxHeaders,
        @DefaultValue("128") int maxHeaderKeyLength,
        @DefaultValue("1024") int maxHeaderValueLength
) {
    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024;
    public static final int DEFAULT_MAX_ROUTING_KEY_LENGTH = 255;
    public static final int DEFAULT_MAX_HEADERS = 64;
    public static final int DEFAULT_MAX_HEADER_KEY_LENGTH = 128;
    public static final int DEFAULT_MAX_HEADER_VALUE_LENGTH = 1024;

    public GrpcPublisherProperties {
        if (maxPayloadBytes <= 0) maxPayloadBytes = DEFAULT_MAX_PAYLOAD_BYTES;
        if (maxRoutingKeyLength <= 0) maxRoutingKeyLength = DEFAULT_MAX_ROUTING_KEY_LENGTH;
        if (maxHeaders <= 0) maxHeaders = DEFAULT_MAX_HEADERS;
        if (maxHeaderKeyLength <= 0) maxHeaderKeyLength = DEFAULT_MAX_HEADER_KEY_LENGTH;
        if (maxHeaderValueLength <= 0) maxHeaderValueLength = DEFAULT_MAX_HEADER_VALUE_LENGTH;
    }

    public static GrpcPublisherProperties defaults() {
        return new GrpcPublisherProperties(
                DEFAULT_MAX_PAYLOAD_BYTES,
                DEFAULT_MAX_ROUTING_KEY_LENGTH,
                DEFAULT_MAX_HEADERS,
                DEFAULT_MAX_HEADER_KEY_LENGTH,
                DEFAULT_MAX_HEADER_VALUE_LENGTH
        );
    }
}
