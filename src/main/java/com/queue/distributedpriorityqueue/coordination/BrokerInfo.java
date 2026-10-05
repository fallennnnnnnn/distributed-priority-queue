package com.queue.distributedpriorityqueue.coordination;

/** A live broker: who it is and where clients and other brokers reach it. */
public record BrokerInfo(String id, String incarnation, String host, int httpPort, int grpcPort) {

    public ReplicaId replicaId() {
        return new ReplicaId(id, incarnation);
    }
}
