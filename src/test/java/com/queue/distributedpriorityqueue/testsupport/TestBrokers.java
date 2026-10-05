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
        return settings(brokerId, httpPort, sweeperInterval, Duration.ofSeconds(10));
    }

    /** @param commitTimeout runs on real time, unlike the message timeouts, which follow the test's clock */
    public static BrokerSettings settings(String brokerId, int httpPort, Duration sweeperInterval,
            Duration commitTimeout) {
        return settings(brokerId, httpPort, httpPort + 1000, sweeperInterval, commitTimeout);
    }

    public static BrokerSettings settings(String brokerId, int httpPort, int grpcPort, Duration sweeperInterval,
            Duration commitTimeout) {
        return new BrokerSettings(brokerId,
                new BrokerSettings.Advertised("localhost", httpPort, grpcPort),
                new BrokerSettings.Coordinator(BrokerSettings.CoordinatorType.MEMORY, List.of()),
                new BrokerSettings.QueueDefaults(List.of("HIGH", "MEDIUM", "LOW"), Duration.ofSeconds(30), 3, null,
                        1, 1, 1, false),
                Duration.ofSeconds(5), Duration.ofSeconds(5), commitTimeout, sweeperInterval, Duration.ofMillis(200));
    }
}
