package com.queue.distributedpriorityqueue.cluster;

import com.queue.distributedpriorityqueue.coordination.Coordinator;
import com.queue.distributedpriorityqueue.coordination.InMemoryCoordinator;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Builds the broker this process runs, with the coordinator its settings ask for. */
@Configuration(proxyBeanMethods = false)
public class ClusterConfiguration {

    @Bean
    Coordinator coordinator(BrokerSettings settings) {
        return switch (settings.coordinator().type()) {
            case MEMORY -> new InMemoryCoordinator();
            case ETCD -> throw new IllegalStateException(
                    "the etcd coordinator is not built yet (work item 5.1); set dpq.coordinator.type=memory");
        };
    }

    @Bean(initMethod = "start")
    Broker broker(BrokerSettings settings, Coordinator coordinator, Clock clock) {
        return new Broker(settings, coordinator, clock);
    }

    @Bean
    QueueService queueService(Broker broker) {
        return broker.queues();
    }
}
