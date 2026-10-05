package com.queue.distributedpriorityqueue.coordination;

import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.util.List;
import java.util.Optional;

/**
 * The cluster's shared, consistent metadata: which brokers are alive, which queues exist and, for
 * every shard, its leader, epoch and in-sync replicas.
 *
 * <p>Updates are compare-and-swap: the caller quotes the version it read, and the update is
 * refused if the value has changed since. That is what lets exactly one replica win an election.
 */
public interface Coordinator extends AutoCloseable {

    /**
     * Announces a live broker. The registration lasts until it is closed or its lease is lost;
     * {@code onLeaseLost} runs in the second case.
     */
    Registration register(BrokerInfo broker, Runnable onLeaseLost);

    List<BrokerInfo> liveBrokers();

    /** Stores a new queue and all its shard states in one step. False if the name is taken. */
    boolean createQueue(QueueConfig config, List<ShardState> shards);

    Optional<Versioned<QueueConfig>> queue(String name);

    List<String> queueNames();

    /** False if the queue's settings changed since {@code expectedVersion} was read. */
    boolean updateQueue(String name, long expectedVersion, QueueConfig config);

    Optional<Versioned<ShardState>> shard(ShardId id);

    /** False if the shard's state changed since {@code expectedVersion} was read. */
    boolean updateShard(ShardId id, long expectedVersion, ShardState state);

    /** Reports every change from now on, in order. Events name what changed; listeners read the new value. */
    Subscription watch(Listener listener);

    @Override
    void close();

    interface Listener {

        default void onBrokersChanged() {
        }

        default void onQueueChanged(String queue) {
        }

        default void onShardChanged(ShardId shard) {
        }
    }

    interface Registration extends AutoCloseable {

        @Override
        void close();
    }

    interface Subscription extends AutoCloseable {

        @Override
        void close();
    }
}
