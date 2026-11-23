package com.volo.broker.broker.services;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.settings.PersistenceMode;
import com.volo.broker.persistence.MessageStore;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class MessageLifecycleService {
    private final MessageStore messageStore;

    public MessageLifecycleService(MessageStore messageStore) {
        this.messageStore = Objects.requireNonNull(messageStore);
    }

    public void complete(BrokerQueue queue, QueuedMessage message) {
        try {
            if (queue != null && queue.settings().persistenceMode() == PersistenceMode.PERSISTENT) {
                messageStore.delete(message.getMessageId());
            }
        } finally {
            if (message != null) {
                message.release();
            }
        }
    }

    public void discard(QueuedMessage message) {
        if (message != null) {
            message.release();
        }
    }
}