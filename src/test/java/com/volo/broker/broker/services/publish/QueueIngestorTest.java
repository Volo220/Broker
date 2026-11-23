package com.volo.broker.broker.services.publish;

import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.flow.QueueFlowController;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.settings.OverflowPolicy;
import com.volo.broker.broker.queue.settings.QueueCapacityPolicy;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class QueueIngestorTest {

    private final MessageStore messageStore = mock(MessageStore.class);
    private final MessageLifecycleService lifecycleService = mock(MessageLifecycleService.class);
    private final GlobalMemoryGuard memoryGuard = mock(GlobalMemoryGuard.class);
    private final QueueFlowController flowController = mock(QueueFlowController.class);

    private final QueueIngestor ingestor = new QueueIngestor(
            messageStore,
            lifecycleService,
            memoryGuard,
            flowController
    );

    @Test
    void ingest_persistentQueue_shouldSaveToStoreAndRecordMemory() {
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("orders.q"), TestQueueSettings.reliable());
        QueuedMessage message = TestMessages.message(1);

        ingestor.ingest(queue, message);

        verify(messageStore).save(eq("orders.q"), eq(message));
        verify(memoryGuard).recordBytesAdded(message.sizeBytes());
    }

    @Test
    void ingest_memoryOnlyQueue_shouldNotSaveToStore() {
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("temp.q"), TestQueueSettings.memoryOnlyNoConsumerAck());
        QueuedMessage message = TestMessages.message(2);

        ingestor.ingest(queue, message);

        verifyNoInteractions(messageStore);
        verify(memoryGuard).recordBytesAdded(message.sizeBytes());
    }

    @Test
    void ingest_whenDropOldestOccurs_shouldCompleteDroppedMessagesAndRemoveBytes() {
        var settings = new QueueSettings(
                com.volo.broker.broker.queue.settings.PersistenceMode.MEMORY_ONLY,
                com.volo.broker.broker.queue.settings.PublishAckMode.ENABLED,
                com.volo.broker.broker.queue.settings.ConsumerAckMode.ENABLED,
                TestQueueSettings.reliable().redeliveryPolicy(),
                TestQueueSettings.reliable().retentionPolicy(),
                TestQueueSettings.reliable().lifecyclePolicy(),
                new QueueCapacityPolicy(true, 1, 0, OverflowPolicy.DROP_OLDEST),
                TestQueueSettings.reliable().dispatchPolicy()
        );
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("drop.q"), settings);
        QueuedMessage msg1 = TestMessages.message(10);
        QueuedMessage msg2 = TestMessages.message(20);

        ingestor.ingest(queue, msg1);
        verify(lifecycleService, never()).complete(any(), any());

        ingestor.ingest(queue, msg2);
        verify(lifecycleService).complete(eq(queue), eq(msg1));
        verify(memoryGuard).recordBytesRemoved(msg1.sizeBytes());
    }
}
