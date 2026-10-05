package com.queue.distributedpriorityqueue.testsupport;

import com.queue.distributedpriorityqueue.coordination.BrokerInfo;
import com.queue.distributedpriorityqueue.coordination.Coordinator;
import com.queue.distributedpriorityqueue.coordination.InMemoryCoordinator;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.coordination.Versioned;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.exception.CoordinatorUnavailableException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * One broker's connection to the coordinator the test cluster shares. A test can cut it: the
 * broker's calls then fail, it hears of no changes, and its lease runs out, so the other brokers
 * see it as gone and the broker itself learns that it has lost its lease.
 */
public final class CoordinatorLink implements Coordinator {

    private final InMemoryCoordinator shared;
    private volatile boolean cut;
    private volatile String brokerId;

    public CoordinatorLink(InMemoryCoordinator shared) {
        this.shared = shared;
    }

    public void cut() {
        cut = true;
        if (brokerId != null) {
            shared.expireLease(brokerId);
        }
    }

    public void restore() {
        cut = false;
    }

    @Override
    public Registration register(BrokerInfo broker, Runnable onLeaseLost) {
        requireReachable();
        brokerId = broker.id();
        return shared.register(broker, onLeaseLost);
    }

    @Override
    public List<BrokerInfo> liveBrokers() {
        requireReachable();
        return shared.liveBrokers();
    }

    @Override
    public boolean createQueue(QueueConfig config, List<ShardState> shards) {
        requireReachable();
        return shared.createQueue(config, shards);
    }

    @Override
    public Optional<Versioned<QueueConfig>> queue(String name) {
        requireReachable();
        return shared.queue(name);
    }

    @Override
    public List<String> queueNames() {
        requireReachable();
        return shared.queueNames();
    }

    @Override
    public boolean updateQueue(String name, long expectedVersion, QueueConfig config) {
        requireReachable();
        return shared.updateQueue(name, expectedVersion, config);
    }

    @Override
    public Optional<Versioned<ShardState>> shard(ShardId id) {
        requireReachable();
        return shared.shard(id);
    }

    @Override
    public OptionalLong updateShard(ShardId id, long expectedVersion, ShardState state) {
        requireReachable();
        return shared.updateShard(id, expectedVersion, state);
    }

    @Override
    public Subscription watch(Listener listener) {
        return shared.watch(new Listener() {
            @Override
            public void onBrokersChanged() {
                if (!cut) {
                    listener.onBrokersChanged();
                }
            }

            @Override
            public void onQueueChanged(String queue) {
                if (!cut) {
                    listener.onQueueChanged(queue);
                }
            }

            @Override
            public void onShardChanged(ShardId shard) {
                if (!cut) {
                    listener.onShardChanged(shard);
                }
            }
        });
    }

    /** The shared coordinator belongs to the cluster, which closes it. */
    @Override
    public void close() {
    }

    private void requireReachable() {
        if (cut) {
            throw new CoordinatorUnavailableException("the coordinator is unreachable");
        }
    }
}
