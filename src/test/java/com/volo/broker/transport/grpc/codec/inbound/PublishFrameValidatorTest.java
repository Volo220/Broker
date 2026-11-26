package com.volo.broker.transport.grpc.codec.inbound;

import com.volo.broker.buffer.Payload;
import com.volo.broker.config.GrpcPublisherProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PublishFrameValidatorTest {

    private GrpcPublisherProperties properties;
    private PublishFrameValidator validator;
    private final Payload validPayload = Payload.of(new byte[]{1, 2, 3});

    @BeforeEach
    void setUp() {
        properties = new GrpcPublisherProperties(1024, 64, 5, 32, 64);
        validator = new PublishFrameValidator(properties);
    }

    @Test
    @DisplayName("Validation succeeds for valid input")
    void validate_shouldPassValidInput() {
        String error = validator.validate("orders.created", validPayload, Map.of("key", "val"));
        assertThat(error).isNull();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t"})
    @DisplayName("Fails when routingKey is null, empty or blank")
    void validate_shouldRejectBlankRoutingKey(String key) {
        String error = validator.validate(key, validPayload, Map.of());
        assertThat(error).isEqualTo("routingKey is required");
    }

    @Test
    @DisplayName("Fails when routingKey exceeds maxRoutingKeyLength")
    void validate_shouldRejectOversizedRoutingKey() {
        String longKey = "a".repeat(65);
        String error = validator.validate(longKey, validPayload, Map.of());
        assertThat(error).isEqualTo("routingKey is too long");
    }

    @Test
    @DisplayName("Fails when payload is null or empty")
    void validate_shouldRejectEmptyPayload() {
        assertThat(validator.validate("route", null, Map.of())).isEqualTo("payload must not be empty");
        assertThat(validator.validate("route", Payload.empty(), Map.of())).isEqualTo("payload must not be empty");
    }

    @Test
    @DisplayName("Fails when headers count exceeds maxHeaders")
    void validate_shouldRejectTooManyHeaders() {
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            headers.put("k" + i, "v" + i);
        }
        String error = validator.validate("route", validPayload, headers);
        assertThat(error).isEqualTo("too many headers");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    @DisplayName("Fails when header key is blank")
    void validate_shouldRejectBlankHeaderKey(String headerKey) {
        Map<String, String> headers = new HashMap<>();
        headers.put(headerKey, "value");
        String error = validator.validate("route", validPayload, headers);
        assertThat(error).isEqualTo("header key must not be blank");
    }

    @Test
    @DisplayName("Fails when header key exceeds maxHeaderKeyLength")
    void validate_shouldRejectOversizedHeaderKey() {
        Map<String, String> headers = Map.of("a".repeat(33), "value");
        String error = validator.validate("route", validPayload, headers);
        assertThat(error).isEqualTo("header key is too long");
    }

    @Test
    @DisplayName("Fails when header value exceeds maxHeaderValueLength")
    void validate_shouldRejectOversizedHeaderValue() {
        Map<String, String> headers = Map.of("key", "v".repeat(65));
        String error = validator.validate("route", validPayload, headers);
        assertThat(error).isEqualTo("header value is too long");
    }
}
