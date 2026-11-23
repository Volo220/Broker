package com.volo.broker.broker.services;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Drains all unconsumed and in-flight messages upon broker shutdown to guarantee
 * deterministic release of off-heap buffers.
 */
@Component
public class BrokerShutdownDrainer {
    private static final Logger log = LoggerFactory.getLogger(BrokerShutdownDrainer.class);

    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;
    private final MessageLifecycleService lifecycleService;

    public BrokerShutdownDrainer(
            QueueManager queueManager,
            SubscriptionManager subscriptionManager,
            MessageLifecycleService lifecycleService
    ) {
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager must not be null");
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager, "subscriptionManager must not be null");
        this.lifecycleService = Objects.requireNonNull(lifecycleService, "lifecycleService must not be null");
    }

    @PreDestroy
    public void drainAllOnShutdown() {
        log.info("Draining broker queues and in-flight messages on shutdown...");
        for (Subscription sub : subscriptionManager.all()) {
            for (QueuedMessage msg : sub.getInFlightTracker().drainAll()) {
                lifecycleService.discard(msg);
            }
        }
        for (BrokerQueue queue : queueManager.all()) {
            for (QueuedMessage msg : queue.drainAll()) {
                lifecycleService.discard(msg);
            }
        }
    }
}
