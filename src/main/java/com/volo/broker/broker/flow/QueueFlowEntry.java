package com.volo.broker.broker.flow;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks flow state, timeout policy, and waiting flow gates for a specific queue.
 */
class QueueFlowEntry {
    volatile FlowState state = FlowState.OPEN;
    volatile long maxBlockMs = 0;
    final Set<FlowGate> waiters = ConcurrentHashMap.newKeySet();
}
