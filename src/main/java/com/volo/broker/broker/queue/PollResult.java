package com.volo.broker.broker.queue;

import com.volo.broker.broker.flow.FlowTransition;
import com.volo.broker.broker.message.QueuedMessage;

import java.util.Objects;
import java.util.Optional;

public final class PollResult {

    private static final PollResult EMPTY_NONE = new PollResult(null, FlowTransition.NONE);

    private final QueuedMessage message;
    private final FlowTransition transition;

    public PollResult(QueuedMessage message, FlowTransition transition) {
        this.message = message;
        this.transition = Objects.requireNonNull(transition, "transition must not be null");
    }

    public static PollResult empty() {
        return EMPTY_NONE;
    }

    public static PollResult empty(FlowTransition transition) {
        return transition == FlowTransition.NONE ? EMPTY_NONE : new PollResult(null, transition);
    }

    public static PollResult of(QueuedMessage message, FlowTransition transition) {
        return new PollResult(message, transition);
    }

    public static PollResult of(QueuedMessage message) {
        return new PollResult(message, FlowTransition.NONE);
    }

    public boolean hasMessage() {
        return message != null;
    }

    public QueuedMessage messageOrNull() {
        return message;
    }

    public Optional<QueuedMessage> message() {
        return Optional.ofNullable(message);
    }

    public FlowTransition transition() {
        return transition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PollResult that)) return false;
        return Objects.equals(message, that.message) && transition == that.transition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, transition);
    }

    @Override
    public String toString() {
        return "PollResult[message=" + message + ", transition=" + transition + "]";
    }
}
