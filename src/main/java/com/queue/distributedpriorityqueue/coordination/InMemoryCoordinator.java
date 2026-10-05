package com.queue.distributedpriorityqueue.coordination;

import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * A coordinator inside the JVM, for the single-broker stack and for tests that run several
 * brokers in one process. Changes are reported on one background thread, in order, the way a
 * remote coordinator's watch delivers them.
 */
public final class InMemoryCoordinator implements Coordinator {

    private final Map<String, LiveBroker> brokers = new HashMap<>();
    private final Map<String, Versioned<QueueConfig>> queues = new HashMap<>();
    private final Map<ShardId, Versioned<ShardState>> shards = new HashMap<>();
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private final ExecutorService watchThread = Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, "coordinator-watch");
        thread.setDaemon(true);
        return thread;
    });
    private long version;

    @Override
    public synchronized Registration register(BrokerInfo broker, Runnable onLeaseLost) {
        if (brokers.containsKey(broker.id())) {
            throw new IllegalStateException("a broker with ID " + broker.id() + " is already registered");
        }
        brokers.put(broker.id(), new LiveBroker(broker, onLeaseLost));
        tell(Listener::onBrokersChanged);
        return () -> deregister(broker);
    }

    @Override
    public synchronized List<BrokerInfo> liveBrokers() {
        return brokers.values().stream().map(LiveBroker::info).sorted(Comparator.comparing(BrokerInfo::id)).toList();
    }

    @Override
    public synchronized boolean createQueue(QueueConfig config, List<ShardState> shardStates) {
        if (queues.containsKey(config.name())) {
            return false;
        }
        if (shardStates.size() != config.shards()) {
            throw new IllegalArgumentException(
                    "queue " + config.name() + " has " + config.shards() + " shards but " + shardStates.size() + " states");
        }
        queues.put(config.name(), new Versioned<>(config, ++version));
        for (int shard = 0; shard < shardStates.size(); shard++) {
            shards.put(new ShardId(config.name(), shard), new Versioned<>(shardStates.get(shard), ++version));
        }
        tell(listener -> listener.onQueueChanged(config.name()));
        return true;
    }

    @Override
    public synchronized Optional<Versioned<QueueConfig>> queue(String name) {
        return Optional.ofNullable(queues.get(name));
    }

    @Override
    public synchronized List<String> queueNames() {
        return queues.keySet().stream().sorted().toList();
    }

    @Override
    public synchronized boolean updateQueue(String name, long expectedVersion, QueueConfig config) {
        Versioned<QueueConfig> current = queues.get(name);
        if (current == null || current.version() != expectedVersion) {
            return false;
        }
        queues.put(name, new Versioned<>(config, ++version));
        tell(listener -> listener.onQueueChanged(name));
        return true;
    }

    @Override
    public synchronized Optional<Versioned<ShardState>> shard(ShardId id) {
        return Optional.ofNullable(shards.get(id));
    }

    @Override
    public synchronized boolean updateShard(ShardId id, long expectedVersion, ShardState state) {
        Versioned<ShardState> current = shards.get(id);
        if (current == null || current.version() != expectedVersion) {
            return false;
        }
        shards.put(id, new Versioned<>(state, ++version));
        tell(listener -> listener.onShardChanged(id));
        return true;
    }

    @Override
    public Subscription watch(Listener listener) {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    /** For tests: the broker stops being live, as if it had stopped renewing its lease. */
    public synchronized void expireLease(String brokerId) {
        LiveBroker expired = brokers.remove(brokerId);
        if (expired != null) {
            watchThread.execute(expired.onLeaseLost());
            tell(Listener::onBrokersChanged);
        }
    }

    @Override
    public void close() {
        watchThread.shutdownNow();
    }

    private synchronized void deregister(BrokerInfo broker) {
        LiveBroker current = brokers.get(broker.id());
        if (current != null && current.info().equals(broker)) {
            brokers.remove(broker.id());
            tell(Listener::onBrokersChanged);
        }
    }

    private void tell(Consumer<Listener> event) {
        List<Listener> current = new ArrayList<>(listeners);
        if (!watchThread.isShutdown()) {
            watchThread.execute(() -> current.forEach(event));
        }
    }

    private record LiveBroker(BrokerInfo info, Runnable onLeaseLost) {
    }
}
