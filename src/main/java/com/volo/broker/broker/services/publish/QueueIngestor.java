package com.volo.broker.broker.services.publish;

import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.flow.QueueFlowController;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.EnqueueResult;
import com.volo.broker.broker.queue.settings.PersistenceMode;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.persistence.MessageStore;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class QueueIngestor {
    private final MessageStore messageStore;
    private final MessageLifecycleService lifecycleService;
    private final @Nullable GlobalMemoryGuard globalMemoryGuard;
    private final @Nullable QueueFlowController flowController;

    public QueueIngestor(
            MessageStore messageStore,
            MessageLifecycleService lifecycleService
    ) {
        this(messageStore, lifecycleService, null, null);
    }

    @Autowired
    public QueueIngestor(
            MessageStore messageStore,
            MessageLifecycleService lifecycleService,
            @Nullable GlobalMemoryGuard globalMemoryGuard,
            @Nullable QueueFlowController flowController
    ) {
        this.messageStore = Objects.requireNonNull(messageStore, "messageStore must not be null");
        this.lifecycleService = Objects.requireNonNull(lifecycleService, "lifecycleService must not be null");
        this.globalMemoryGuard = globalMemoryGuard;
        this.flowController = flowController;
    }

    public void ingest(BrokerQueue queue, QueuedMessage message) {
        if (queue.settings().persistenceMode() == PersistenceMode.PERSISTENT) {
            messageStore.save(queue.queuePath(), message);
        }

        EnqueueResult enqueueResult = queue.enqueue(message);
        for (QueuedMessage dropped : enqueueResult.droppedMessages()) {
            lifecycleService.complete(queue, dropped);
            recordBytesRemoved(dropped.sizeBytes());
        }

        recordBytesAdded(message.sizeBytes());
    }

    private void recordBytesRemoved(int bytes) {
        if (globalMemoryGuard != null) {
            FlowTransition gt = globalMemoryGuard.recordBytesRemoved(bytes);
            if (flowController != null) {
                flowController.handleGlobalTransition(gt);
            }
        }
    }

    private void recordBytesAdded(int bytes) {
        if (globalMemoryGuard != null) {
            FlowTransition gt = globalMemoryGuard.recordBytesAdded(bytes);
            if (flowController != null) {
                flowController.handleGlobalTransition(gt);
            }
        }
    }
}
