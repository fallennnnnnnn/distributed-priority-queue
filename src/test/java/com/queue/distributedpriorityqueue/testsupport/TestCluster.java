package com.queue.distributedpriorityqueue.testsupport;

import com.queue.distributedpriorityqueue.cluster.Broker;
import com.queue.distributedpriorityqueue.cluster.BrokerSettings;
import com.queue.distributedpriorityqueue.cluster.QueueService;
import com.queue.distributedpriorityqueue.coordination.InMemoryCoordinator;
import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Several brokers in one JVM. They share one coordinator, one network that tests can break, and
 * one clock that tests move by hand. Brokers are named broker-1, broker-2 and so on.
 */
public final class TestCluster implements AutoCloseable {

    public final ManualClock clock = new ManualClock();
    public final InMemoryCoordinator coordinator = new InMemoryCoordinator();
    public final InProcessNetwork network = new InProcessNetwork();
    private final Function<String, BrokerSettings> settings;
    private final Map<String, Broker> brokers = new LinkedHashMap<>();

    public TestCluster(int size) {
        this(size, Duration.ofSeconds(10));
    }

    /** @param commitTimeout runs on real time, so tests of it use a short one */
    public TestCluster(int size, Duration commitTimeout) {
        this.settings = brokerId -> TestBrokers.settings(brokerId, 8080, Duration.ofHours(1), commitTimeout);
        for (int number = 1; number <= size; number++) {
            start("broker-" + number);
        }
    }

    /** Starts a broker. A broker started again under the same ID is a new incarnation with empty logs. */
    public Broker start(String brokerId) {
        Broker broker = new Broker(settings.apply(brokerId), coordinator, network.transportFor(brokerId), clock);
        brokers.put(brokerId, broker);
        network.register(brokerId, broker.api());
        broker.start();
        return broker;
    }

    /** Shuts a broker down cleanly: it leaves the network and tells the coordinator it is gone. */
    public void stop(String brokerId) {
        Broker broker = brokers.remove(brokerId);
        network.unregister(brokerId);
        broker.close();
    }

    public Broker broker(String brokerId) {
        return brokers.get(brokerId);
    }

    public QueueService queues(String brokerId) {
        return broker(brokerId).queues();
    }

    public List<Broker> brokers() {
        return new ArrayList<>(brokers.values());
    }

    /** The broker the coordinator names as the shard's leader. */
    public Broker leaderOf(ShardId shard) {
        ReplicaId leader = coordinator.shard(shard).orElseThrow().value().leader();
        return broker(leader.brokerId());
    }

    /** The running brokers that hold a replica of the shard without leading it. */
    public List<Broker> followersOf(ShardId shard) {
        Broker leader = leaderOf(shard);
        return brokers.values().stream()
                .filter(broker -> broker != leader && broker.replica(shard).isPresent())
                .toList();
    }

    public ShardReplica replica(String brokerId, ShardId shard) {
        return broker(brokerId).replica(shard).orElseThrow();
    }

    @Override
    public void close() {
        brokers.values().forEach(Broker::close);
        network.close();
        coordinator.close();
    }
}
