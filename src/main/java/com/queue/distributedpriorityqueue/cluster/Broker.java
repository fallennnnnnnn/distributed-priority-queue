package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.BrokerInfo;
import com.queue.distributedpriorityqueue.coordination.Coordinator;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One instance of the service. It registers with the coordinator, holds the shard replicas
 * assigned to it and sweeps them for timeouts. Several brokers can run in one JVM, each with its
 * own settings, which is how the multi-broker tests work.
 */
public final class Broker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final BrokerInfo info;
    private final Coordinator coordinator;
    private final Duration sweeperInterval;
    private final ClusterMetadata metadata;
    private final LocalShards localShards;
    private final QueueService queues;
    private final ScheduledExecutorService scheduler;
    private Coordinator.Registration registration;
    private Coordinator.Subscription subscription;

    public Broker(BrokerSettings settings, Coordinator coordinator, Clock clock) {
        this.info = new BrokerInfo(settings.brokerId(), UUID.randomUUID().toString(), settings.advertised().host(),
                settings.advertised().httpPort(), settings.advertised().grpcPort());
        this.coordinator = coordinator;
        this.sweeperInterval = settings.sweeperInterval();
        this.metadata = new ClusterMetadata(coordinator);
        this.localShards = new LocalShards(info.id(), clock);
        this.metadata.onRefresh(localShards::sync);
        this.scheduler = Executors.newScheduledThreadPool(2, task -> {
            Thread thread = new Thread(task, "dpq-" + info.id());
            thread.setDaemon(true);
            return thread;
        });
        this.queues = new QueueService(coordinator, metadata, localShards, new Router(), scheduler);
    }

    public void start() {
        registration = coordinator.register(info, () -> { });
        subscription = coordinator.watch(new Coordinator.Listener() {
            @Override
            public void onQueueChanged(String queue) {
                metadata.refresh(queue);
            }

            @Override
            public void onShardChanged(ShardId shard) {
                metadata.refresh(shard.queue());
            }
        });
        coordinator.queueNames().forEach(metadata::refresh);
        long interval = sweeperInterval.toMillis();
        scheduler.scheduleWithFixedDelay(this::sweep, interval, interval, TimeUnit.MILLISECONDS);
        log.info("Broker {} started as incarnation {}", info.id(), info.incarnation());
    }

    public BrokerInfo info() {
        return info;
    }

    public QueueService queues() {
        return queues;
    }

    /** Calls the listener for every shard replica this broker holds now or creates later. */
    public void onReplicaCreated(Consumer<ShardReplica> listener) {
        localShards.onCreated(listener);
    }

    /** Reclaims timed-out and expired messages on every shard, so idle shards still make progress. */
    public void sweep() {
        for (ShardReplica replica : localShards.all()) {
            try {
                replica.reclaim();
            } catch (RuntimeException e) {
                log.warn("Sweeping shard {}/{} failed", replica.config().name(), replica.shard(), e);
            }
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        if (subscription != null) {
            subscription.close();
        }
        if (registration != null) {
            registration.close();
        }
    }
}
