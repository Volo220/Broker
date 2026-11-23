package com.volo.broker.broker.command.handler;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.command.CommandHandler;
import com.volo.broker.broker.command.model.DisconnectCommand;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.services.MessageLifecycleService;
import com.volo.broker.broker.subscription.SubscriptionManager;
import lombok.AllArgsConstructor;

@Component
@AllArgsConstructor
public class DisconnectCommandHandler implements CommandHandler<DisconnectCommand, Void> {

    private final SubscriptionManager subscriptionManager;
    private final QueueManager queueManager;
    private final QueueDispatchScheduler dispatchScheduler;
    private final MessageLifecycleService messageLifecycleService;
    @Override
    public Class<DisconnectCommand> getCommandType() {
        return DisconnectCommand.class;
    }

    @Override
    public Void handle(DisconnectCommand cmd) {
        subscriptionManager.findByConsumerTag(cmd.consumerTag())
                .ifPresent(this::disconnectSubscription);
        return null;
    }

    private void disconnectSubscription(com.volo.broker.broker.subscription.Subscription sub) {
        subscriptionManager.unregister(sub.consumerTag());

        List<QueuedMessage> drained = new ArrayList<>(sub.getInFlightTracker().drainAll());
        dispatchScheduler.signal(sub.queueName());

        var maybeQueue = queueManager.find(sub.queueName());
        for (int i = drained.size() - 1; i >= 0; i--) {
            processDrainedMessage(drained.get(i), maybeQueue);
        }
    }

    private void processDrainedMessage(QueuedMessage message, java.util.Optional<BrokerQueue> maybeQueue) {
        if (maybeQueue.isEmpty()) {
            messageLifecycleService.discard(message);
            return;
        }

        BrokerQueue queue = maybeQueue.get();
        boolean shouldRequeue = queue.settings().redeliveryPolicy().requeueOnDisconnect()
                && queue.canRedeliver(message);

        if (shouldRequeue) {
            queue.requeue(message.asRedelivered());
        } else {
            messageLifecycleService.complete(queue, message);
        }
    }
}
