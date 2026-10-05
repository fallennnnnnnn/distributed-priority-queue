package com.queue.distributedpriorityqueue.metrics;

import com.queue.distributedpriorityqueue.cluster.Broker;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToDoubleFunction;
import org.springframework.stereotype.Component;

/**
 * Publishes the numbers of each shard this broker leads, as meters tagged with queue, shard and
 * priority level. A broker publishes a shard's meters while it leads the shard and removes them
 * when it stops, so no message is counted twice. Prometheus adds them up per queue with
 * {@code sum by (queue)}, and takes {@code max by (queue)} for the oldest message age.
 */
@Component
class QueueMeters {

    /** One scrape reads many meters from the same shard, so they share a snapshot for this long. */
    private static final Duration SNAPSHOT_AGE = Duration.ofSeconds(1);

    private final MeterRegistry registry;
    /** What is published per replica. It also keeps the sources alive, which Micrometer holds only weakly. */
    private final Map<ShardReplica, Published> published = new ConcurrentHashMap<>();

    QueueMeters(MeterRegistry registry, Broker broker) {
        this.registry = registry;
        broker.onLeading(this::publish);
        broker.onNoLongerLeading(this::withdraw);
    }

    private void publish(ShardReplica replica) {
        Snapshot snapshot = new Snapshot(replica);
        List<Meter> meters = new ArrayList<>();
        Tags tags = Tags.of("queue", replica.config().name(), "shard", String.valueOf(replica.shard()));

        List<String> levels = replica.config().priorityLevels();
        for (int rank = 0; rank < levels.size(); rank++) {
            int level = rank;
            meters.add(Gauge.builder("dpq.messages.ready", snapshot, s -> s.get().readyByPriority().get(level))
                    .description("Messages ready to be consumed")
                    .tags(tags.and("priority", levels.get(rank)))
                    .register(registry));
        }
        meters.add(gauge("dpq.messages.in.flight", "Messages dequeued and not yet acknowledged", tags, snapshot,
                s -> s.get().inFlight()));
        meters.add(Gauge.builder("dpq.oldest.message.age", snapshot, s -> s.get().oldestReadyAge().toMillis() / 1000.0)
                .description("Age of the oldest message that is ready to be consumed")
                .baseUnit("seconds")
                .tags(tags)
                .register(registry));
        meters.add(gauge("dpq.dead.letters", "Messages currently in the dead-letter queue", tags, snapshot,
                s -> s.get().deadLetters()));
        meters.add(gauge("dpq.messages.enqueued.per.second", "Enqueue rate over the last 60 seconds", tags, snapshot,
                s -> s.get().enqueuedPerSecond()));
        meters.add(gauge("dpq.messages.acked.per.second", "Acknowledge rate over the last 60 seconds", tags, snapshot,
                s -> s.get().ackedPerSecond()));

        meters.add(counter("dpq.messages.enqueued", "Messages enqueued", tags, snapshot, s -> s.get().enqueuedTotal()));
        meters.add(counter("dpq.messages.delivered", "Deliveries, including redeliveries", tags, snapshot,
                s -> s.get().deliveredTotal()));
        meters.add(counter("dpq.messages.redelivered", "Deliveries after the first", tags, snapshot,
                s -> s.get().redeliveredTotal()));
        meters.add(counter("dpq.messages.acked", "Messages acknowledged", tags, snapshot, s -> s.get().ackedTotal()));
        meters.add(counter("dpq.messages.dead.lettered", "Messages moved to the dead-letter queue", tags, snapshot,
                s -> s.get().deadLetteredTotal()));
        meters.add(counter("dpq.messages.expired", "Messages dropped because their TTL passed", tags, snapshot,
                s -> s.get().expiredTotal()));

        meters.add(gauge("dpq.in.sync.replicas", "Replicas that have every committed event", tags, replica,
                r -> r.status().isr().size()));
        meters.add(gauge("dpq.under.replicated", "1 when the shard has fewer in-sync replicas than its replication factor",
                tags, replica, r -> r.status().isr().size() < r.config().replicationFactor() ? 1 : 0));
        meters.add(gauge("dpq.leader.epoch", "Goes up by one with every new leader", tags, replica,
                r -> r.status().epoch()));
        meters.add(gauge("dpq.log.end.offset", "The offset the leader's next event will get", tags, replica,
                r -> r.status().logEnd()));
        meters.add(gauge("dpq.high.watermark", "Events below this offset are on every in-sync replica", tags, replica,
                r -> r.status().highWatermark()));
        meters.add(counter("dpq.leader.elections", "Elections this broker won for the shard", tags, replica,
                ShardReplica::electionsWon));
        meters.add(counter("dpq.shard.resets", "Elections won without being in sync, which may have lost messages",
                tags, replica, ShardReplica::resets));

        Published previous = published.put(replica, new Published(snapshot, meters));
        if (previous != null) {
            previous.meters().forEach(registry::remove);
        }
    }

    private void withdraw(ShardReplica replica) {
        Published gone = published.remove(replica);
        if (gone != null) {
            gone.meters().forEach(registry::remove);
        }
    }

    private <T> Meter gauge(String name, String description, Tags tags, T source, ToDoubleFunction<T> value) {
        return Gauge.builder(name, source, value).description(description).tags(tags).register(registry);
    }

    private <T> Meter counter(String name, String description, Tags tags, T source, ToDoubleFunction<T> value) {
        return FunctionCounter.builder(name, source, value).description(description).tags(tags).register(registry);
    }

    private record Published(Snapshot snapshot, List<Meter> meters) {
    }

    private static final class Snapshot {

        private final ShardReplica replica;
        private ShardMetrics metrics;
        private long takenAtNanos;

        Snapshot(ShardReplica replica) {
            this.replica = replica;
        }

        synchronized ShardMetrics get() {
            long now = System.nanoTime();
            if (metrics == null || now - takenAtNanos > SNAPSHOT_AGE.toNanos()) {
                try {
                    metrics = replica.metrics();
                } catch (RuntimeException e) {
                    // The replica stopped leading a moment ago and its meters are about to be withdrawn.
                    if (metrics == null) {
                        metrics = replica.snapshot().metrics();
                    }
                }
                takenAtNanos = now;
            }
            return metrics;
        }
    }
}
