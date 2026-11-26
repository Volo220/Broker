package com.volo.broker.broker.services;

import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.command.handler.DisconnectCommandHandler;
import com.volo.broker.broker.command.model.AckCommand;
import com.volo.broker.broker.command.model.NackCommand;
import com.volo.broker.broker.command.model.PublishCommand;
import com.volo.broker.broker.command.model.PublishResult;
import com.volo.broker.broker.dispatch.DispatchManager;
import com.volo.broker.broker.dispatch.QueueDispatchScheduler;
import com.volo.broker.broker.message.GlobalLongMessageIdGenerator;
import com.volo.broker.broker.message.MessageDelivery;
import com.volo.broker.broker.queue.InMemoryQueueManager;
import com.volo.broker.broker.queue.QueueManager;
import com.volo.broker.broker.queue.settings.OverflowPolicy;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.broker.services.publish.PublishService;
import com.volo.broker.broker.subscription.ConsumerChannel;
import com.volo.broker.broker.subscription.DefaultSubscriptionManager;
import com.volo.broker.broker.validation.QueuePathValidator;
import com.volo.broker.buffer.TestCountingPayload;
import com.volo.broker.persistence.MessageStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MessageOwnershipIntegrationTest {

    private QueueManager queueManager;
    private DefaultSubscriptionManager subscriptionManager;
    private MessageStore messageStore;
    private MessageLifecycleService lifecycleService;
    private PublishService publishService;
    private DispatchManager dispatchManager;
    private ConsumerAcknowledgementService ackService;
    private QueueDeclarationService declarationService;
    private BrokerShutdownDrainer shutdownDrainer;
    @BeforeEach
    void setUp() {
        queueManager = new InMemoryQueueManager();
        subscriptionManager = new DefaultSubscriptionManager();
        messageStore = mock(MessageStore.class);
        lifecycleService = new MessageLifecycleService(messageStore);
        QueueDispatchScheduler scheduler = mock(QueueDispatchScheduler.class);

        publishService = new PublishService(
                queueManager, messageStore, scheduler,
                new GlobalLongMessageIdGenerator(), lifecycleService
        );
        dispatchManager = new DispatchManager(queueManager, subscriptionManager, lifecycleService);
        ackService = new ConsumerAcknowledgementService(
                subscriptionManager, queueManager, scheduler, lifecycleService
        );
        QueuePersistenceService persistenceService = mock(QueuePersistenceService.class);
        declarationService = new QueueDeclarationService(
                queueManager, new QueuePathValidator(), persistenceService, lifecycleService, null
        );
        shutdownDrainer = new BrokerShutdownDrainer(queueManager, subscriptionManager, lifecycleService);
        new DisconnectCommandHandler(
                subscriptionManager, queueManager, scheduler, lifecycleService
        );
    }

    @Test
    void singleQueuePublishAndAck_shouldRetainAndReleaseCleanly() {
        queueManager.declare(QueuePath.of("orders.created"), TestQueueSettings.reliable());
        TestCountingPayload payload = new TestCountingPayload("order-1".getBytes(), 1);

        PublishResult res = publishWithTransportRelease(new PublishCommand(1L, "orders.created", payload, Map.of()));
        assertThat(res.accepted()).isTrue();
        assertThat(payload.refCnt()).isEqualTo(1); // Owned by queue

        TestConsumerChannel channel = new TestConsumerChannel("c1");
        subscriptionManager.register("orders.created", channel, 10);
        dispatchManager.dispatchLoop("orders.created");

        assertThat(channel.deliveries).hasSize(1);
        assertThat(payload.refCnt()).isEqualTo(1); // In-flight

        MessageDelivery delivery = channel.deliveries.get(0);
        ackService.ack(new AckCommand("c1", delivery.deliveryTag(), delivery.message().getMessageId()));
        assertThat(payload.refCnt()).isZero(); // Fully freed
    }

    @Test
    void multiQueueFanout_shouldRetainPerQueueAndReleasePerAck() {
        queueManager.declare(QueuePath.of("orders.eu"), TestQueueSettings.reliable());
        queueManager.declare(QueuePath.of("orders.us"), TestQueueSettings.reliable());
        TestCountingPayload payload = new TestCountingPayload("fanout-data".getBytes(), 1);

        PublishResult res = publishWithTransportRelease(new PublishCommand(1L, "orders.*", payload, Map.of()));
        assertThat(res.routedQueuePaths()).hasSize(2);
        assertThat(payload.refCnt()).isEqualTo(2); // 1 per queue

        TestConsumerChannel c1 = new TestConsumerChannel("c1");
        TestConsumerChannel c2 = new TestConsumerChannel("c2");
        subscriptionManager.register("orders.eu", c1, 10);
        subscriptionManager.register("orders.us", c2, 10);
        dispatchManager.dispatchLoop("orders.eu");
        dispatchManager.dispatchLoop("orders.us");

        MessageDelivery d1 = c1.deliveries.get(0);
        ackService.ack(new AckCommand("c1", d1.deliveryTag(), d1.message().getMessageId()));
        assertThat(payload.refCnt()).isEqualTo(1);

        MessageDelivery d2 = c2.deliveries.get(0);
        ackService.ack(new AckCommand("c2", d2.deliveryTag(), d2.message().getMessageId()));
        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void unmatchedRoutingKey_shouldReleaseIncomingPayload() {
        TestCountingPayload payload = new TestCountingPayload("unmatched".getBytes(), 1);
        PublishResult res = publishWithTransportRelease(new PublishCommand(1L, "unknown.queue", payload, Map.of()));

        assertThat(res.accepted()).isFalse();
        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void dropOldest_shouldReleaseDroppedMessagePayload() {
        QueueSettings settings = TestQueueSettings.withCapacity(1, OverflowPolicy.DROP_OLDEST);
        queueManager.declare(QueuePath.of("cap.q"), settings);

        TestCountingPayload p1 = new TestCountingPayload("p1".getBytes(), 1);
        TestCountingPayload p2 = new TestCountingPayload("p2".getBytes(), 1);

        publishWithTransportRelease(new PublishCommand(1L, "cap.q", p1, Map.of()));
        assertThat(p1.refCnt()).isEqualTo(1);

        publishWithTransportRelease(new PublishCommand(2L, "cap.q", p2, Map.of()));
        assertThat(p1.refCnt()).isZero(); // Dropped & freed
        assertThat(p2.refCnt()).isEqualTo(1); // In queue
    }

    @Test
    void nackWithAndWithoutRequeue_shouldHandlePayloadCorrectly() {
        queueManager.declare(QueuePath.of("retry.q"), TestQueueSettings.reliable());
        TestCountingPayload payload = new TestCountingPayload("data".getBytes(), 1);
        publishWithTransportRelease(new PublishCommand(1L, "retry.q", payload, Map.of()));

        TestConsumerChannel channel = new TestConsumerChannel("c1");
        subscriptionManager.register("retry.q", channel, 10);
        dispatchManager.dispatchLoop("retry.q");

        MessageDelivery d1 = channel.deliveries.get(0);
        // NACK with requeue
        ackService.nack(new NackCommand("c1", d1.deliveryTag(), d1.message().getMessageId(), true));
        assertThat(payload.refCnt()).isEqualTo(1); // Requeued

        // Re-dispatch & NACK without requeue
        channel.deliveries.clear();
        dispatchManager.dispatchLoop("retry.q");
        MessageDelivery d2 = channel.deliveries.get(0);
        ackService.nack(new NackCommand("c1", d2.deliveryTag(), d2.message().getMessageId(), false));
        assertThat(payload.refCnt()).isZero(); // Discarded
    }

    @Test
    void deleteQueueAndShutdown_shouldDrainAllPayloads() {
        declarationService.declare("del.q", TestQueueSettings.reliable());
        TestCountingPayload p1 = new TestCountingPayload("backlog".getBytes(), 1);
        publishWithTransportRelease(new PublishCommand(1L, "del.q", p1, Map.of()));

        declarationService.delete("del.q");
        assertThat(p1.refCnt()).isZero(); // Drained & freed

        // Test shutdown drainer
        queueManager.declare(QueuePath.of("shut.q"), TestQueueSettings.reliable());
        TestCountingPayload p2 = new TestCountingPayload("shutdown-msg".getBytes(), 1);
        publishWithTransportRelease(new PublishCommand(2L, "shut.q", p2, Map.of()));

        shutdownDrainer.drainAllOnShutdown();
        assertThat(p2.refCnt()).isZero();
    }

    @Test
    void ackAndDisconnectAfterQueueDeleted_shouldDiscardWithoutLeak() {
        queueManager.declare(QueuePath.of("orphan.q"), TestQueueSettings.reliable());
        TestCountingPayload p1 = new TestCountingPayload("msg-1".getBytes(), 1);
        publishWithTransportRelease(new PublishCommand(1L, "orphan.q", p1, Map.of()));

        TestConsumerChannel channel = new TestConsumerChannel("c1");
        subscriptionManager.register("orphan.q", channel, 10);
        dispatchManager.dispatchLoop("orphan.q");

        // Queue deleted while in-flight
        queueManager.delete(QueuePath.of("orphan.q"));

        // ACK missing queue
        MessageDelivery delivery = channel.deliveries.get(0);
        ackService.ack(new AckCommand("c1", delivery.deliveryTag(), delivery.message().getMessageId()));
        assertThat(p1.refCnt()).isZero();
    }

    private PublishResult publishWithTransportRelease(PublishCommand cmd) {
        try {
            return publishService.publish(cmd);
        } finally {
            cmd.payload().release();
        }
    }

    private static class TestConsumerChannel implements ConsumerChannel {
        private final String tag;
        private final List<MessageDelivery> deliveries = new ArrayList<>();

        TestConsumerChannel(String tag) { this.tag = tag; }
        @Override public String consumerTag() { return tag; }
        @Override public boolean canAcceptMore() { return true; }
        @Override public void send(MessageDelivery delivery) { deliveries.add(delivery); }
        @Override public void close(String reason) {}
        @Override public void onAck(long deliveryTag) {}
        @Override public void onNack(long deliveryTag, boolean requeue) {}
    }
}
