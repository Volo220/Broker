package com.volo.broker.broker.dispatch;

import java.util.Optional;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.message.MessageDelivery;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.settings.ConsumerAckMode;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.PollResult;
import com.volo.broker.broker.subscription.Subscription;
import com.volo.broker.broker.subscription.SubscriptionManager;
import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.flow.GlobalMemoryGuard;
import com.volo.broker.broker.flow.QueueFlowController;

import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;

@Component
public class DispatchManager {
    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;
    private final MessageLifecycleService lifecycleService;
    private final QueueFlowController flowController;
    private final GlobalMemoryGuard globalMemoryGuard;

    public DispatchManager(
            QueueManager queueManager,
            SubscriptionManager subscriptionManager,
            MessageLifecycleService lifecycleService
    ) {
        this(queueManager, subscriptionManager, lifecycleService, null, null);
    }

    @Autowired
    public DispatchManager(
            QueueManager queueManager,
            SubscriptionManager subscriptionManager,
            MessageLifecycleService lifecycleService,
            @Nullable QueueFlowController flowController,
            @Nullable GlobalMemoryGuard globalMemoryGuard
    ) {
        this.queueManager = queueManager;
        this.subscriptionManager = subscriptionManager;
        this.lifecycleService = lifecycleService;
        this.flowController = flowController;
        this.globalMemoryGuard = globalMemoryGuard;
    }
    
    public void dispatchLoop(String queueName) {
        while (true) {
            var maybeQueue = queueManager.find(queueName);
            if (maybeQueue.isEmpty() || !dispatchOne(maybeQueue.get())) {
                return;
            }
        }
    }

    private boolean dispatchOne(BrokerQueue queue) {
        Optional<Subscription> maybeSubscription = subscriptionManager.nextReady(
                queue.queuePath(),
                queue.settings().dispatchPolicy().consumerSelectionStrategy()
        );

        if (maybeSubscription.isEmpty() || !maybeSubscription.get().canAcceptMore()) {
            return false;
        }

        PollResult pollResult = queue.poll();
        QueuedMessage message = pollResult.messageOrNull();
        if (message == null) {
            return false;
        }

        updateFlowOnPoll(queue, pollResult, message);

        if (queue.isExpired(message)) {
            lifecycleService.complete(queue, message);
            return true;
        }

        dispatch(queue, maybeSubscription.get(), message);
        return true;
    }

    private void updateFlowOnPoll(BrokerQueue queue, PollResult pollResult, QueuedMessage message) {
        if (flowController != null) {
            flowController.handleTransition(queue.queuePath(), pollResult.transition());
        }
        if (globalMemoryGuard != null) {
            FlowTransition gt = globalMemoryGuard.recordBytesRemoved(message.sizeBytes());
            if (flowController != null) {
                flowController.handleGlobalTransition(gt);
            }
        }
    }
    private void dispatch(
            BrokerQueue queue,
            Subscription subscription,
            QueuedMessage message
    ) {
        long deliveryTag = subscription.nextDeliveryTag();
        boolean consumerAckRequired = queue.settings().consumerAckMode() == ConsumerAckMode.ENABLED;

        if (consumerAckRequired) {
            subscription.track(deliveryTag, message);
        }

        try {
            subscription.channel().send(MessageDelivery.of(deliveryTag, message));

            queue.touch();

            if (!consumerAckRequired) {
                lifecycleService.complete(queue, message);
            }

        } catch (Throwable throwable) {
             if (consumerAckRequired) {
                subscription.remove(deliveryTag);
            }

            boolean shouldRequeue = queue.settings().redeliveryPolicy().requeueOnSendFailure()
                    && queue.canRedeliver(message);

            if (shouldRequeue) {
                queue.requeue(message.asRedelivered());
                return;
            }

            lifecycleService.complete(queue, message);
        }
    }

}
