package com.queue.distributedpriorityqueue.testsupport;

import com.queue.distributedpriorityqueue.cluster.BrokerSettings;
import java.time.Duration;
import java.util.List;

/** Broker settings for tests that build brokers by hand instead of through Spring. */
public final class TestBrokers {

    private TestBrokers() {
    }

    /** The README defaults, except for the ID and ports, and with the sweeper effectively off. */
    public static BrokerSettings settings(String brokerId, int httpPort) {
        return settings(brokerId, httpPort, Duration.ofHours(1));
    }

    public static BrokerSettings settings(String brokerId, int httpPort, Duration sweeperInterval) {
        return new BrokerSettings(brokerId,
                new BrokerSettings.Advertised("localhost", httpPort, httpPort + 1000),
                new BrokerSettings.Coordinator(BrokerSettings.CoordinatorType.MEMORY, List.of()),
                new BrokerSettings.QueueDefaults(List.of("HIGH", "MEDIUM", "LOW"), Duration.ofSeconds(30), 3, null,
                        1, 1, 1, false),
                Duration.ofSeconds(5), Duration.ofSeconds(5), sweeperInterval, Duration.ofMillis(200));
    }
}
