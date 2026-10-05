package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.BrokerInfo;

/** How brokers reach each other. */
public interface BrokerTransport extends AutoCloseable {

    /** A transport for a broker that never has peers to talk to. */
    BrokerTransport NONE = new BrokerTransport() {
        @Override
        public BrokerApi connect(BrokerInfo target) {
            throw new IllegalStateException("no broker-to-broker transport is configured; cannot reach " + target.id());
        }

        @Override
        public void close() {
        }
    };

    /** An object whose calls are carried out by the target broker. */
    BrokerApi connect(BrokerInfo target);

    @Override
    void close();
}
