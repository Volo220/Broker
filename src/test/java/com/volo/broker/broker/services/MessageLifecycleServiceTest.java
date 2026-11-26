package com.volo.broker.broker.services;

import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.Map;

class MessageLifecycleServiceTest {

    private final MessageStore messageStore = mock(MessageStore.class);
    private final MessageLifecycleService service = new MessageLifecycleService(messageStore);

    @Test
    void complete_shouldDeletePersistentMessage() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        service.complete(queue, TestMessages.message(100));

        verify(messageStore).delete(100L);
    }

    @Test
    void complete_shouldNotDeleteMemoryOnlyMessage() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.memoryOnlyNoConsumerAck()
        );

        service.complete(queue, TestMessages.message(100));

        verifyNoInteractions(messageStore);
    }

    @Test
    void complete_shouldReleasePayloadEvenIfStoreThrows() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );
        com.volo.broker.buffer.TestCountingPayload payload =
                new com.volo.broker.buffer.TestCountingPayload("data".getBytes(), 1);
        QueuedMessage message = new QueuedMessage(
                1L,
                payload,
                Map.of(),
                "orders.created",
                System.currentTimeMillis(),
                false,
                0
        );

        doThrow(new RuntimeException("disk error")).when(messageStore).delete(1L);

        try {
            service.complete(queue, message);
        } catch (RuntimeException ignored) {
        }

        assertThat(payload.refCnt()).isZero();
        assertThat(payload.releaseCalls()).isEqualTo(1);
    }

    @Test
    void discard_shouldReleasePayload() {
        com.volo.broker.buffer.TestCountingPayload payload =
                new com.volo.broker.buffer.TestCountingPayload("data".getBytes(), 1);
        QueuedMessage message = new QueuedMessage(
                1L,
                payload,
                Map.of(),
                "orders.created",
                System.currentTimeMillis(),
                false,
                0
        );

        service.discard(message);

        assertThat(payload.refCnt()).isZero();
        assertThat(payload.releaseCalls()).isEqualTo(1);
    }
}
