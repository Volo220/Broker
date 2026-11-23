package com.volo.broker.broker.services.publish;

import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.error.BrokerErrorCode;
import com.volo.broker.broker.error.BrokerException;
import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.flow.QueueFlowController;
import com.volo.broker.broker.message.MessageIdGenerator;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.exception.QueueOverflowException;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.persistence.MessageStore;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class PublishService {
    private final QueueManager queueManager;
    private final QueueIngestor queueIngestor;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageIdGenerator messageIdGenerator;

    @Autowired
    public PublishService(
            QueueManager queueManager,
            QueueIngestor queueIngestor,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator
    ) {
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.queueIngestor = Objects.requireNonNull(queueIngestor, "queueIngestor must not be null");
        this.dispatchScheduler = Objects.requireNonNull(dispatchScheduler, "dispatchScheduler must not be null");
        this.messageIdGenerator = Objects.requireNonNull(messageIdGenerator, "messageIdGenerator must not be null");
    }

    public PublishService(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator
    ) {
        this(queueManager, messageStore, dispatchScheduler, messageIdGenerator, new MessageLifecycleService(messageStore), null, null);
    }

    public PublishService(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator,
            MessageLifecycleService lifecycleService
    ) {
        this(queueManager, messageStore, dispatchScheduler, messageIdGenerator, lifecycleService, null, null);
    }

    public PublishService(
            QueueManager queueManager,
            MessageStore messageStore,
            QueueDispatchScheduler dispatchScheduler,
            MessageIdGenerator messageIdGenerator,
            MessageLifecycleService lifecycleService,
            @Nullable QueueFlowController flowController,
            @Nullable GlobalMemoryGuard globalMemoryGuard
    ) {
        this(
                queueManager,
                new QueueIngestor(messageStore, lifecycleService, globalMemoryGuard, flowController),
                dispatchScheduler,
                messageIdGenerator
        );
    }

    public PublishResult publish(PublishCommand command) {
        try {
            validate(command);

            List<BrokerQueue> queues = queueManager.resolve(command.routingKey());
            if (queues.isEmpty()) {
                return PublishResult.nack(
                        command.clientPublishId(),
                        BrokerErrorCode.QUEUE_NOT_FOUND,
                        "No queue matched routing key: " + command.routingKey()
                );
            }

            List<QueuedMessage> messages = queues.stream()
                    .map(queue -> createMessage(command))
                    .toList();

            checkCapacity(queues, messages);
            List<String> routedQueuePaths = routeToQueues(queues, messages);
            return PublishResult.ack(command.clientPublishId(), routedQueuePaths);
        } catch (Throwable throwable) {
            return handlePublishError(throwable, command.clientPublishId());
        }
    }

    private void checkCapacity(List<BrokerQueue> queues, List<QueuedMessage> messages) {
        for (int i = 0; i < queues.size(); i++) {
            BrokerQueue queue = queues.get(i);
            QueuedMessage message = messages.get(i);
            if (!queue.canAccept(message)) {
                throw new QueueOverflowException("Queue capacity exceeded: " + queue.queuePath());
            }
        }
    }

    private List<String> routeToQueues(List<BrokerQueue> queues, List<QueuedMessage> messages) {
        List<String> routedQueuePaths = new ArrayList<>(queues.size());
        for (int i = 0; i < queues.size(); i++) {
            BrokerQueue queue = queues.get(i);
            QueuedMessage message = messages.get(i);

            queueIngestor.ingest(queue, message);
            routedQueuePaths.add(queue.queuePath());
            dispatchScheduler.signal(queue.queuePath());
        }
        return routedQueuePaths;
    }

    private PublishResult handlePublishError(Throwable throwable, Long clientPublishId) {
        if (throwable instanceof QueueOverflowException ex) {
            return PublishResult.nack(clientPublishId, BrokerErrorCode.QUEUE_OVERFLOW, ex.getMessage());
        } else if (throwable instanceof IllegalArgumentException ex) {
            return PublishResult.nack(clientPublishId, BrokerErrorCode.INVALID_REQUEST, ex.getMessage());
        } else if (throwable instanceof BrokerException ex) {
            return PublishResult.nack(clientPublishId, ex.code(), ex.getMessage());
        } else {
            return PublishResult.nack(clientPublishId, BrokerErrorCode.INTERNAL_ERROR, "Message was not accepted by broker");
        }
    }

    private QueuedMessage createMessage(PublishCommand command) {
        return new QueuedMessage(
                messageIdGenerator.nextId(),
                command.payload(),
                command.headers(),
                command.routingKey(),
                System.currentTimeMillis(),
                false,
                0
        );
    }

    private void validate(PublishCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("publish command must not be null");
        }

        if (command.routingKey() == null || command.routingKey().isBlank()) {
            throw new IllegalArgumentException("routingKey is required");
        }

        if (command.payload().isEmpty()) {
            throw new IllegalArgumentException("payload must not be empty");
        }
    }
}
