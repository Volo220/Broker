package com.volo.broker.broker.command.model;

import com.volo.broker.broker.command.TransportCommand;
import com.volo.broker.broker.message.Payload;

import java.util.Map;

public record PublishCommand(
    Long clientPublishId,
    String routingKey,
    Payload payload,
    Map<String, String> headers
) implements TransportCommand {

    public PublishCommand(
            Long clientPublishId,
            String routingKey,
            Payload payload,
            Map<String, String> headers
    ) {
        this.clientPublishId = clientPublishId;
        this.routingKey = routingKey == null ? "" : routingKey.trim();
        this.payload = payload == null ? Payload.empty() : payload;
        this.headers = normalizeHeaders(headers);
    }

    public PublishCommand(
            Long clientPublishId,
            String routingKey,
            byte[] payload,
            Map<String, String> headers
    ) {
        this(clientPublishId, routingKey, Payload.of(payload), headers);
    }

    private static Map<String, String> normalizeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        return Map.copyOf(headers);
    }
}
