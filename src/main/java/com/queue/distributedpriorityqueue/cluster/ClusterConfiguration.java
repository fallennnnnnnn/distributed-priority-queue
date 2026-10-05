package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.cluster.BrokerSettings.CoordinatorType;
import com.queue.distributedpriorityqueue.coordination.Coordinator;
import com.queue.distributedpriorityqueue.coordination.EtcdCoordinator;
import com.queue.distributedpriorityqueue.coordination.InMemoryCoordinator;
import com.queue.distributedpriorityqueue.transport.GrpcBrokerServer;
import com.queue.distributedpriorityqueue.transport.GrpcBrokerTransport;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the broker this process runs. {@code dpq.coordinator.type} decides what kind:
 *
 * <ul>
 *   <li>{@code memory}: a single broker. Its coordinator lives inside the process and it has no
 *       other brokers to reach.
 *   <li>{@code etcd}: one broker of a cluster. It shares an etcd with the others and talks to
 *       them over gRPC.
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class ClusterConfiguration {

    @Bean
    Coordinator coordinator(BrokerSettings settings) {
        return switch (settings.coordinator().type()) {
            case MEMORY -> new InMemoryCoordinator();
            case ETCD -> new EtcdCoordinator(settings.coordinator().endpoints(), settings.failureDetectionTimeout());
        };
    }

    @Bean
    BrokerTransport brokerTransport(BrokerSettings settings) {
        // A passed-on request must be allowed to outlast the leader's own wait for its followers.
        return settings.coordinator().type() == CoordinatorType.ETCD
                ? new GrpcBrokerTransport(settings.commitTimeout().plusSeconds(5))
                : BrokerTransport.NONE;
    }

    @Bean(initMethod = "start")
    Broker broker(BrokerSettings settings, Coordinator coordinator, BrokerTransport transport, Clock clock) {
        return new Broker(settings, coordinator, transport, clock);
    }

    @Bean
    QueueService queueService(Broker broker) {
        return broker.queues();
    }

    @Bean
    @ConditionalOnProperty(name = "dpq.coordinator.type", havingValue = "etcd")
    GrpcBrokerServer grpcBrokerServer(Broker broker, BrokerSettings settings) {
        return new GrpcBrokerServer(broker.api(), settings.advertised().grpcPort());
    }
}
