package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.BrokerInfo;
import com.queue.distributedpriorityqueue.coordination.Coordinator;
import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/** The other live brokers, as the coordinator lists them, and the connection to each. */
final class Peers {

    private final ReplicaId self;
    private final Coordinator coordinator;
    private final BrokerTransport transport;
    private final Map<ReplicaId, BrokerApi> connections = new ConcurrentHashMap<>();
    private volatile Map<ReplicaId, BrokerInfo> live = Map.of();

    Peers(ReplicaId self, Coordinator coordinator, BrokerTransport transport) {
        this.self = self;
        this.coordinator = coordinator;
        this.transport = transport;
    }

    /** Reads the live brokers again and forgets the connections to those that are gone. */
    void refresh() {
        live = coordinator.liveBrokers().stream()
                .filter(broker -> !broker.replicaId().equals(self))
                .collect(Collectors.toUnmodifiableMap(BrokerInfo::replicaId, Function.identity()));
        connections.keySet().retainAll(live.keySet());
    }

    /** The connection to that broker process, or empty if it is not live. */
    Optional<BrokerApi> find(ReplicaId replica) {
        BrokerInfo broker = live.get(replica);
        if (broker == null) {
            // It may have registered after the last change report arrived.
            refresh();
            broker = live.get(replica);
        }
        return Optional.ofNullable(broker).map(this::connection);
    }

    List<BrokerApi> all() {
        return live.values().stream().map(this::connection).toList();
    }

    private BrokerApi connection(BrokerInfo broker) {
        return connections.computeIfAbsent(broker.replicaId(), replica -> transport.connect(broker));
    }
}
