package com.queue.distributedpriorityqueue.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.queue.distributedpriorityqueue.cluster.BrokerSettings.CoordinatorType;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class BrokerSettingsTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner().withUserConfiguration(BrokerConfiguration.class);

    @Test
    void defaultsMatchTheReadme() {
        runner.run(context -> {
            BrokerSettings settings = context.getBean(BrokerSettings.class);

            assertThat(settings.brokerId()).isEqualTo("broker-1");
            assertThat(settings.advertised().host()).isEqualTo("localhost");
            assertThat(settings.advertised().httpPort()).isEqualTo(8080);
            assertThat(settings.advertised().grpcPort()).isEqualTo(9090);
            assertThat(settings.coordinator().type()).isEqualTo(CoordinatorType.MEMORY);
            assertThat(settings.coordinator().endpoints()).isEmpty();
            assertThat(settings.failureDetectionTimeout()).isEqualTo(Duration.ofSeconds(5));
            assertThat(settings.replicaLagMax()).isEqualTo(Duration.ofSeconds(5));
            assertThat(settings.commitTimeout()).isEqualTo(Duration.ofSeconds(10));
            assertThat(settings.sweeperInterval()).isEqualTo(Duration.ofMillis(100));
            assertThat(settings.summaryInterval()).isEqualTo(Duration.ofMillis(200));

            BrokerSettings.QueueDefaults queue = settings.queueDefaults();
            assertThat(queue.priorityLevels()).containsExactly("HIGH", "MEDIUM", "LOW");
            assertThat(queue.visibilityTimeout()).isEqualTo(Duration.ofSeconds(30));
            assertThat(queue.maxRetries()).isEqualTo(3);
            assertThat(queue.defaultTtl()).isNull();
            assertThat(queue.shards()).isEqualTo(1);
            assertThat(queue.replicationFactor()).isEqualTo(1);
            assertThat(queue.minInSyncReplicas()).isEqualTo(1);
            assertThat(queue.uncleanLeaderElection()).isFalse();
        });
    }

    @Test
    void queueDefaultsMatchTheCoreDefaults() {
        runner.run(context -> {
            BrokerSettings.QueueDefaults queue = context.getBean(BrokerSettings.class).queueDefaults();
            QueueConfig core = QueueConfig.builder("orders").build();

            assertThat(queue.priorityLevels()).isEqualTo(core.priorityLevels());
            assertThat(queue.visibilityTimeout()).isEqualTo(core.visibilityTimeout());
            assertThat(queue.maxRetries()).isEqualTo(core.maxRetries());
            assertThat(queue.defaultTtl()).isEqualTo(core.defaultTtl());
            assertThat(queue.shards()).isEqualTo(core.shards());
            assertThat(queue.replicationFactor()).isEqualTo(core.replicationFactor());
            assertThat(queue.minInSyncReplicas()).isEqualTo(core.minInSyncReplicas());
            assertThat(queue.uncleanLeaderElection()).isEqualTo(core.uncleanLeaderElection());
        });
    }

    @Test
    void bindsOverrides() {
        runner.withPropertyValues(
                "dpq.broker-id=broker-2",
                "dpq.advertised.host=broker-2.internal",
                "dpq.advertised.http-port=8081",
                "dpq.advertised.grpc-port=9091",
                "dpq.coordinator.type=etcd",
                "dpq.coordinator.endpoints=http://etcd-1:2379,http://etcd-2:2379",
                "dpq.failure-detection-timeout=3s",
                "dpq.queue-defaults.priority-levels=urgent,normal",
                "dpq.queue-defaults.default-ttl=10m",
                "dpq.queue-defaults.shards=4",
                "dpq.queue-defaults.replication-factor=3",
                "dpq.queue-defaults.min-in-sync-replicas=2",
                "dpq.queue-defaults.unclean-leader-election=true")
                .run(context -> {
                    BrokerSettings settings = context.getBean(BrokerSettings.class);

                    assertThat(settings.brokerId()).isEqualTo("broker-2");
                    assertThat(settings.advertised())
                            .isEqualTo(new BrokerSettings.Advertised("broker-2.internal", 8081, 9091));
                    assertThat(settings.coordinator().type()).isEqualTo(CoordinatorType.ETCD);
                    assertThat(settings.coordinator().endpoints())
                            .containsExactly("http://etcd-1:2379", "http://etcd-2:2379");
                    assertThat(settings.failureDetectionTimeout()).isEqualTo(Duration.ofSeconds(3));
                    assertThat(settings.queueDefaults().priorityLevels()).containsExactly("urgent", "normal");
                    assertThat(settings.queueDefaults().defaultTtl()).isEqualTo(Duration.ofMinutes(10));
                    assertThat(settings.queueDefaults().shards()).isEqualTo(4);
                    assertThat(settings.queueDefaults().replicationFactor()).isEqualTo(3);
                    assertThat(settings.queueDefaults().minInSyncReplicas()).isEqualTo(2);
                    assertThat(settings.queueDefaults().uncleanLeaderElection()).isTrue();
                });
    }

    @Test
    void advertisedHttpPortFollowsServerPort() {
        runner.withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("server.port=8081")
                .run(context -> assertThat(context.getBean(BrokerSettings.class).advertised().httpPort())
                        .isEqualTo(8081));
    }

    @Test
    void providesTheSystemClock() {
        runner.run(context -> assertThat(context.getBean(Clock.class)).isEqualTo(Clock.systemUTC()));
    }

    @Test
    void etcdNeedsEndpoints() {
        assertStartupFails("endpoints are required when type is etcd", "dpq.coordinator.type=etcd");
    }

    @Test
    void minInSyncReplicasCannotExceedReplicationFactor() {
        assertStartupFails("min-in-sync-replicas must not exceed replication-factor",
                "dpq.queue-defaults.replication-factor=2", "dpq.queue-defaults.min-in-sync-replicas=3");
    }

    @Test
    void priorityLevelsMustBeUnique() {
        assertStartupFails("priority level names must be unique",
                "dpq.queue-defaults.priority-levels=HIGH,LOW,HIGH");
    }

    @Test
    void atMostTenPriorityLevels() {
        assertStartupFails("priorityLevels",
                "dpq.queue-defaults.priority-levels=a,b,c,d,e,f,g,h,i,j,k");
    }

    @Test
    void portsMustBeValid() {
        assertStartupFails("grpcPort", "dpq.advertised.grpc-port=70000");
    }

    @Test
    void intervalsMustBePositive() {
        assertStartupFails("sweeperInterval", "dpq.sweeper-interval=0ms");
    }

    @Test
    void brokerIdMustNotBeBlank() {
        assertStartupFails("brokerId", "dpq.broker-id= ");
    }

    @Test
    void shardsMustBeAtLeastOne() {
        assertStartupFails("shards", "dpq.queue-defaults.shards=0");
    }

    private void assertStartupFails(String expectedInError, String... properties) {
        runner.withPropertyValues(properties).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasStackTraceContaining(expectedInError);
        });
    }
}
