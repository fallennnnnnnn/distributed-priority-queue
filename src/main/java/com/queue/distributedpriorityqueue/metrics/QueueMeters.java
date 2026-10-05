package com.queue.distributedpriorityqueue.metrics;

import com.queue.distributedpriorityqueue.cluster.Broker;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.ToDoubleFunction;
import org.springframework.stereotype.Component;

/**
 * Publishes the numbers of each shard this broker leads, as meters tagged with queue, shard and
 * priority level. Followers publish nothing, so no message is counted twice. Prometheus
 * adds them up per queue with {@code sum by (queue)}, and takes {@code max by (queue)} for the
 * oldest message age.
 */
@Component
class QueueMeters {

    /** One scrape reads many meters from the same shard, so they share a snapshot for this long. */
    private static final Duration SNAPSHOT_AGE = Duration.ofSeconds(1);

    private final MeterRegistry registry;
    /** Micrometer holds its sources weakly; this keeps them alive. */
    private final List<Snapshot> snapshots = new CopyOnWriteArrayList<>();

    QueueMeters(MeterRegistry registry, Broker broker) {
        this.registry = registry;
        broker.onLeading(this::bind);
    }

    private void bind(ShardReplica replica) {
        Snapshot snapshot = new Snapshot(replica);
        snapshots.add(snapshot);
        Tags tags = Tags.of("queue", replica.config().name(), "shard", String.valueOf(replica.shard()));

        List<String> levels = replica.config().priorityLevels();
        for (int rank = 0; rank < levels.size(); rank++) {
            int level = rank;
            Gauge.builder("dpq.messages.ready", snapshot, s -> s.get().readyByPriority().get(level))
                    .description("Messages ready to be consumed")
                    .tags(tags.and("priority", levels.get(rank)))
                    .register(registry);
        }
        gauge("dpq.messages.in.flight", "Messages dequeued and not yet acknowledged", tags, snapshot,
                s -> s.get().inFlight());
        Gauge.builder("dpq.oldest.message.age", snapshot, s -> s.get().oldestReadyAge().toMillis() / 1000.0)
                .description("Age of the oldest message that is ready to be consumed")
                .baseUnit("seconds")
                .tags(tags)
                .register(registry);
        gauge("dpq.dead.letters", "Messages currently in the dead-letter queue", tags, snapshot,
                s -> s.get().deadLetters());
        gauge("dpq.messages.enqueued.per.second", "Enqueue rate over the last 60 seconds", tags, snapshot,
                s -> s.get().enqueuedPerSecond());
        gauge("dpq.messages.acked.per.second", "Acknowledge rate over the last 60 seconds", tags, snapshot,
                s -> s.get().ackedPerSecond());

        counter("dpq.messages.enqueued", "Messages enqueued", tags, snapshot, s -> s.get().enqueuedTotal());
        counter("dpq.messages.delivered", "Deliveries, including redeliveries", tags, snapshot,
                s -> s.get().deliveredTotal());
        counter("dpq.messages.redelivered", "Deliveries after the first", tags, snapshot,
                s -> s.get().redeliveredTotal());
        counter("dpq.messages.acked", "Messages acknowledged", tags, snapshot, s -> s.get().ackedTotal());
        counter("dpq.messages.dead.lettered", "Messages moved to the dead-letter queue", tags, snapshot,
                s -> s.get().deadLetteredTotal());
        counter("dpq.messages.expired", "Messages dropped because their TTL passed", tags, snapshot,
                s -> s.get().expiredTotal());
    }

    private void gauge(String name, String description, Tags tags, Snapshot snapshot, ToDoubleFunction<Snapshot> value) {
        Gauge.builder(name, snapshot, value).description(description).tags(tags).register(registry);
    }

    private void counter(String name, String description, Tags tags, Snapshot snapshot,
            ToDoubleFunction<Snapshot> value) {
        FunctionCounter.builder(name, snapshot, value).description(description).tags(tags).register(registry);
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
                metrics = replica.metrics();
                takenAtNanos = now;
            }
            return metrics;
        }
    }
}
