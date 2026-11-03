package com.volo.broker.broker.inflight;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.collections.Long2ObjectHashMap;

import com.volo.broker.broker.message.QueuedMessage;

public class DefaultInFlightTracker implements InFlightTracker {
    private final AtomicLong counter = new AtomicLong(0);
    private final Long2ObjectHashMap<QueuedMessage> inflight = new Long2ObjectHashMap<>();

    @Override
    public long nextTag() {
        return counter.incrementAndGet();
    }

    @Override
    public synchronized void track(long deliveryTag, QueuedMessage message) {
        inflight.put(deliveryTag, message);
    }

    @Override
    public synchronized QueuedMessage remove(long deliveryTag) {
        return inflight.remove(deliveryTag);
    }

    @Override
    public synchronized Collection<QueuedMessage> drainAll() {
        if (inflight.isEmpty()) {
            return List.of();
        }
        int size = inflight.size();
        long[] tags = new long[size];
        int idx = 0;
        var iterator = inflight.keySet().iterator();
        while (iterator.hasNext()) {
            tags[idx++] = iterator.nextLong();
        }
        Arrays.sort(tags, 0, idx);

        List<QueuedMessage> drained = new ArrayList<>(idx);
        for (int i = 0; i < idx; i++) {
            QueuedMessage msg = inflight.get(tags[i]);
            if (msg != null) {
                drained.add(msg);
            }
        }
        inflight.clear();
        return drained;
    }

    @Override
    public synchronized int size() {
        return inflight.size();
    }
}
