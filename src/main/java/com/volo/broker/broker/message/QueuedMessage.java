package com.volo.broker.broker.message;

import java.util.Map;

import lombok.Getter;

@Getter
public final class QueuedMessage {

    private final long messageId;
    private final Payload payload;
    private final Map<String, String> headers;
    private final String routingKey;
    private final long createdAt;
    private final boolean redelivered;
    private final int deliveryAttempt;
    private final int sizeBytes;

    public QueuedMessage(
            long messageId,
            Payload payload,
            Map<String, String> headers,
            String routingKey,
            long createdAt,
            boolean redelivered,
            int deliveryAttempt
    ) {
        this.messageId = messageId;
        this.payload = payload == null ? Payload.empty() : payload;
        this.headers = normalizeHeaders(headers);
        this.routingKey = routingKey;
        this.createdAt = createdAt;
        this.redelivered = redelivered;
        this.deliveryAttempt = Math.max(0, deliveryAttempt);
        this.sizeBytes = computeSizeBytes(this.payload, this.headers);
    }

    public QueuedMessage(
            long messageId,
            byte[] payload,
            Map<String, String> headers,
            String routingKey,
            long createdAt,
            boolean redelivered,
            int deliveryAttempt
    ) {
        this(
                messageId,
                payload == null || payload.length == 0 ? Payload.empty() : Payload.of(payload),
                headers,
                routingKey,
                createdAt,
                redelivered,
                deliveryAttempt
        );
    }

    public QueuedMessage asRedelivered() {
        return new QueuedMessage(
                messageId,
                payload,
                headers,
                routingKey,
                createdAt,
                true,
                deliveryAttempt + 1
        );
    }

    public int sizeBytes() {
        return sizeBytes;
    }

    private static int computeSizeBytes(Payload payload, Map<String, String> headers) {
        int headersBytes = 0;
        if (!headers.isEmpty()) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                headersBytes += entry.getKey().length() + entry.getValue().length();
            }
        }
        return payload.size() + headersBytes;
    }

    private static Map<String, String> normalizeHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        return Map.copyOf(headers);
    }
}
