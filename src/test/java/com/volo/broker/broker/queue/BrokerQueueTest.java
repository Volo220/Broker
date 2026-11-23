package com.volo.broker.broker.queue;

import com.volo.broker.TestMessages;
import com.volo.broker.TestQueueSettings;
import com.volo.broker.broker.queue.exception.QueueOverflowException;
import com.volo.broker.broker.queue.settings.ConsumerAckMode;
import com.volo.broker.broker.queue.settings.DispatchPolicy;
import com.volo.broker.broker.queue.settings.MessageRetentionPolicy;
import com.volo.broker.broker.queue.settings.OverflowPolicy;
import com.volo.broker.broker.queue.settings.PersistenceMode;
import com.volo.broker.broker.queue.settings.PublishAckMode;
import com.volo.broker.broker.queue.settings.QueueCapacityPolicy;
import com.volo.broker.broker.queue.settings.QueueLifecyclePolicy;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.broker.queue.settings.RedeliveryPolicy;
import com.volo.broker.broker.router.QueuePath;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class BrokerQueueTest {

    @Test
    void poll_shouldReturnMessagesInFifoOrder() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        queue.enqueue(TestMessages.message(1));
        queue.enqueue(TestMessages.message(2));

        assertThat(queue.poll().message()).hasValueSatisfying(message ->
                assertThat(message.getMessageId()).isEqualTo(1)
        );

        assertThat(queue.poll().message()).hasValueSatisfying(message ->
                assertThat(message.getMessageId()).isEqualTo(2)
        );
    }

    @Test
    void requeue_shouldReturnMessageToHead() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        queue.enqueue(TestMessages.message(1));
        queue.enqueue(TestMessages.message(2));

        var first = queue.poll().message().orElseThrow();
        queue.requeue(first.asRedelivered());

        assertThat(queue.poll().message()).hasValueSatisfying(message -> {
            assertThat(message.getMessageId()).isEqualTo(1);
            assertThat(message.isRedelivered()).isTrue();
        });
    }

    @Test
    void enqueue_shouldRejectPublish_whenQueueIsFull() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.withCapacity(1, OverflowPolicy.REJECT_PUBLISH)
        );

        queue.enqueue(TestMessages.message(1));

        assertThatThrownBy(() -> queue.enqueue(TestMessages.message(2)))
                .isInstanceOf(QueueOverflowException.class);
    }

    @Test
    void enqueue_shouldDropOldest_whenOverflowPolicyDropOldest() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.withCapacity(1, OverflowPolicy.DROP_OLDEST)
        );

        EnqueueResult firstResult = queue.enqueue(TestMessages.message(1));
        EnqueueResult secondResult = queue.enqueue(TestMessages.message(2));

        assertThat(firstResult.accepted()).isTrue();
        assertThat(secondResult.accepted()).isTrue();
        assertThat(secondResult.droppedMessages())
                .extracting("messageId")
                .containsExactly(1L);

        assertThat(queue.poll().message()).hasValueSatisfying(message ->
                assertThat(message.getMessageId()).isEqualTo(2)
        );
    }

    @Test
    void enqueue_shouldRejectNewest_whenOverflowPolicyDropNewest() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("metrics.runtime"),
                TestQueueSettings.withCapacity(1, OverflowPolicy.DROP_NEWEST)
        );

        queue.enqueue(TestMessages.message(1));

        assertThatThrownBy(() -> queue.enqueue(TestMessages.message(2)))
                .isInstanceOf(QueueOverflowException.class);

        assertThat(queue.poll().message()).hasValueSatisfying(message ->
                assertThat(message.getMessageId()).isEqualTo(1)
        );
    }

    @Test
    void canAccept_shouldReturnTrueWhenCapacityAvailableAndFalseWhenFull() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.limited"),
                TestQueueSettings.withCapacity(1, OverflowPolicy.REJECT_PUBLISH)
        );

        assertThat(queue.canAccept(TestMessages.message(1))).isTrue();
        queue.enqueue(TestMessages.message(1));
        assertThat(queue.canAccept(TestMessages.message(2))).isFalse();
    }

    @Test
    void requeue_shouldNotBeRejectedEvenWhenQueueIsFull() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.limited"),
                TestQueueSettings.withCapacity(1, OverflowPolicy.REJECT_PUBLISH)
        );

        queue.enqueue(TestMessages.message(1));
        // Even though capacity is 1 and queue has 1 message, requeue of a redelivery must succeed
        assertThatNoException().isThrownBy(() ->
                queue.requeue(TestMessages.message(99).asRedelivered())
        );
        assertThat(queue.size()).isEqualTo(2);
    }

    @Test
    void enqueue_dropOldest_whenIncomingMessageExceedsMaxBytes_shouldNotEvictExistingMessages() {
        var capacityPolicy = new com.volo.broker.broker.queue.settings.QueueCapacityPolicy(
                true,
                10,
                100L,
                OverflowPolicy.DROP_OLDEST
        );
        var settings = new com.volo.broker.broker.queue.settings.QueueSettings(
                PersistenceMode.MEMORY_ONLY,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                RedeliveryPolicy.disabled(),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                capacityPolicy,
                DispatchPolicy.defaults()
        );
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("orders.drop-oldest"), settings);

        var payload1 = new com.volo.broker.buffer.TestCountingPayload(new byte[50], 1);
        var msg1 = new com.volo.broker.broker.message.QueuedMessage(
                1L, payload1, java.util.Map.of(), "orders.drop-oldest", System.currentTimeMillis(), false, 0
        );
        queue.enqueue(msg1);

        assertThat(queue.size()).isEqualTo(1);
        // Initial ref (1) + queue.enqueue (+1) = 2
        assertThat(payload1.refCnt()).isEqualTo(2);

        var payload2 = new com.volo.broker.buffer.TestCountingPayload(new byte[150], 1);
        var msg2 = new com.volo.broker.broker.message.QueuedMessage(
                2L, payload2, java.util.Map.of(), "orders.drop-oldest", System.currentTimeMillis(), false, 0
        );

        assertThatThrownBy(() -> queue.enqueue(msg2))
                .isInstanceOf(QueueOverflowException.class);

        // Invariant check: msg1 MUST NOT have been evicted or lost!
        // Queue size must still be 1, and msg1 payload must still be in the queue with refCnt=2!
        assertThat(queue.size()).isEqualTo(1);
        assertThat(payload1.refCnt()).isEqualTo(2);
        assertThat(queue.poll().message()).hasValueSatisfying(m -> assertThat(m.getMessageId()).isEqualTo(1L));
    }

    @Test
    void isExpired_shouldReturnTrueWhenTtlExceeded() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.withTtl(1)
        );

        assertThat(queue.isExpired(TestMessages.expiredMessage(1))).isTrue();
    }

    @Test
    void isExpired_shouldReturnFalseWhenTtlDisabled() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        assertThat(queue.isExpired(TestMessages.expiredMessage(1))).isFalse();
    }

    @Test
    void canRedeliver_shouldReturnTrueWhenAttemptsBelowLimit() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliable()
        );

        assertThat(queue.canRedeliver(TestMessages.message(1))).isTrue();
    }

    @Test
    void canRedeliver_shouldReturnFalseWhenRedeliveryDisabled() {
        BrokerQueue queue = BrokerQueue.create(
                QueuePath.of("orders.created"),
                TestQueueSettings.reliableWithRedeliveryDisabled()
        );

        assertThat(queue.canRedeliver(TestMessages.message(1))).isFalse();
    }

    @Test
    void canRedeliver_shouldReturnFalseWhenMaxAttemptsReached() {
        var settings = new QueueSettings(
                PersistenceMode.MEMORY_ONLY,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                new RedeliveryPolicy(true, 2, true, true),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                DispatchPolicy.defaults()
        );
        BrokerQueue queue = BrokerQueue.create(QueuePath.of("orders.created"), settings);

        var message = new com.volo.broker.broker.message.QueuedMessage(
                1L, "payload".getBytes(), java.util.Map.of(), "orders.created", System.currentTimeMillis(), true, 2
        );

        assertThat(queue.canRedeliver(message)).isFalse();
    }
}