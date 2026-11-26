package com.volo.broker.transport.grpc.codec.inbound;

import com.custombroker.grpc.ClosePublisherAction;
import com.custombroker.grpc.KeepAlive;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.buffer.Payload;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * Inbound transport frame representing a parsed PublisherRequest.
 */
public sealed interface PublisherFrame permits PublisherFrame.Publish, PublisherFrame.Ping,
        PublisherFrame.Close, PublisherFrame.Invalid {

    default void release() {
    }

    record Publish(
            long clientPublishId,
            String routingKey,
            Payload payload,
            Map<String, String> headers,
            @Nullable BrokerErrorCode rejectCode,
            @Nullable String rejectReason
    ) implements PublisherFrame {

        public Publish {
            headers = headers == null ? Map.of() : Map.copyOf(headers);
        }

        @Override
        public void release() {
            if (payload != null) {
                payload.release();
            }
        }
    }

    record Ping(KeepAlive keepAlive) implements PublisherFrame {
    }

    record Close(ClosePublisherAction action) implements PublisherFrame {
    }

    record Invalid(String reason) implements PublisherFrame {
    }
}
