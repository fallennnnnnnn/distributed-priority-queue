package com.queue.distributedpriorityqueue.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;

import com.queue.distributedpriorityqueue.core.QueueConfig;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class InMemoryCoordinatorTest {

    private static final BrokerInfo BROKER_1 = new BrokerInfo("broker-1", "first", "localhost", 8080, 9090);
    private static final BrokerInfo BROKER_2 = new BrokerInfo("broker-2", "first", "localhost", 8081, 9091);
    private static final ShardId SHARD = new ShardId("orders", 0);

    private final InMemoryCoordinator coordinator = new InMemoryCoordinator();

    @AfterEach
    void close() {
        coordinator.close();
    }

    @Test
    void listsLiveBrokersById() {
        coordinator.register(BROKER_2, () -> { });
        coordinator.register(BROKER_1, () -> { });

        assertThat(coordinator.liveBrokers()).containsExactly(BROKER_1, BROKER_2);
    }

    @Test
    void aBrokerIdCanBeRegisteredOnlyOnce() {
        coordinator.register(BROKER_1, () -> { });

        assertThatIllegalStateException().isThrownBy(
                () -> coordinator.register(new BrokerInfo("broker-1", "second", "localhost", 8080, 9090), () -> { }));
    }

    @Test
    void closingARegistrationRemovesTheBroker() {
        Coordinator.Registration registration = coordinator.register(BROKER_1, () -> { });

        registration.close();

        assertThat(coordinator.liveBrokers()).isEmpty();
    }

    @Test
    void closingAnOldRegistrationLeavesANewIncarnationAlone() {
        Coordinator.Registration old = coordinator.register(BROKER_1, () -> { });
        coordinator.expireLease("broker-1");
        BrokerInfo restarted = new BrokerInfo("broker-1", "second", "localhost", 8080, 9090);
        coordinator.register(restarted, () -> { });

        old.close();

        assertThat(coordinator.liveBrokers()).containsExactly(restarted);
    }

    @Test
    void anExpiredLeaseRemovesTheBrokerAndTellsIt() {
        AtomicInteger leaseLost = new AtomicInteger();
        coordinator.register(BROKER_1, leaseLost::incrementAndGet);

        coordinator.expireLease("broker-1");

        assertThat(coordinator.liveBrokers()).isEmpty();
        await().untilAsserted(() -> assertThat(leaseLost).hasValue(1));
    }

    @Test
    void createsAQueueWithItsShardsInOneStep() {
        QueueConfig config = QueueConfig.builder("orders").shards(2).build();

        assertThat(coordinator.createQueue(config, List.of(state("a", 0), state("b", 0)))).isTrue();

        assertThat(coordinator.queue("orders")).map(Versioned::value).contains(config);
        assertThat(coordinator.shard(new ShardId("orders", 0))).map(Versioned::value).contains(state("a", 0));
        assertThat(coordinator.shard(new ShardId("orders", 1))).map(Versioned::value).contains(state("b", 0));
        assertThat(coordinator.queueNames()).containsExactly("orders");
    }

    @Test
    void aQueueNameCanBeTakenOnlyOnce() {
        QueueConfig config = QueueConfig.builder("orders").build();
        coordinator.createQueue(config, List.of(state("a", 0)));

        assertThat(coordinator.createQueue(config.toBuilder().maxRetries(9).build(), List.of(state("b", 0)))).isFalse();

        assertThat(coordinator.queue("orders")).map(Versioned::value).contains(config);
        assertThat(coordinator.shard(SHARD)).map(Versioned::value).contains(state("a", 0));
    }

    @Test
    void aQueueNeedsOneStatePerShard() {
        assertThatIllegalArgumentException().isThrownBy(() -> coordinator.createQueue(
                QueueConfig.builder("orders").shards(2).build(), List.of(state("a", 0))));
        assertThat(coordinator.queue("orders")).isEmpty();
    }

    @Test
    void unknownNamesAreEmpty() {
        assertThat(coordinator.queue("nope")).isEmpty();
        assertThat(coordinator.shard(new ShardId("nope", 0))).isEmpty();
        assertThat(coordinator.updateQueue("nope", 1, QueueConfig.builder("nope").build())).isFalse();
        assertThat(coordinator.updateShard(new ShardId("nope", 0), 1, state("a", 0))).isEmpty();
    }

    @Test
    void queueSettingsChangeOnlyFromTheVersionThatWasRead() {
        QueueConfig config = QueueConfig.builder("orders").build();
        coordinator.createQueue(config, List.of(state("a", 0)));
        long version = coordinator.queue("orders").orElseThrow().version();
        QueueConfig changed = config.toBuilder().maxRetries(9).build();

        assertThat(coordinator.updateQueue("orders", version, changed)).isTrue();
        assertThat(coordinator.updateQueue("orders", version, config)).isFalse();

        assertThat(coordinator.queue("orders")).map(Versioned::value).contains(changed);
        assertThat(coordinator.queue("orders").orElseThrow().version()).isGreaterThan(version);
    }

    @Test
    void onlyOneOfTwoRacingShardUpdatesWins() {
        coordinator.createQueue(QueueConfig.builder("orders").build(), List.of(state("a", 0)));
        long version = coordinator.shard(SHARD).orElseThrow().version();

        OptionalLong first = coordinator.updateShard(SHARD, version, state("b", 1));
        OptionalLong second = coordinator.updateShard(SHARD, version, state("c", 1));

        assertThat(first).isPresent();
        assertThat(second).isEmpty();
        assertThat(coordinator.shard(SHARD)).contains(new Versioned<>(state("b", 1), first.getAsLong()));
    }

    @Test
    void watchersHearEveryChangeInOrder() {
        List<String> heard = new CopyOnWriteArrayList<>();
        coordinator.watch(new Coordinator.Listener() {
            @Override
            public void onBrokersChanged() {
                heard.add("brokers");
            }

            @Override
            public void onQueueChanged(String queue) {
                heard.add("queue " + queue);
            }

            @Override
            public void onShardChanged(ShardId shard) {
                heard.add("shard " + shard);
            }
        });

        Coordinator.Registration registration = coordinator.register(BROKER_1, () -> { });
        QueueConfig config = QueueConfig.builder("orders").build();
        coordinator.createQueue(config, List.of(state("a", 0)));
        coordinator.updateQueue("orders", coordinator.queue("orders").orElseThrow().version(), config);
        coordinator.updateShard(SHARD, coordinator.shard(SHARD).orElseThrow().version(), state("b", 1));
        registration.close();

        await().untilAsserted(() -> assertThat(heard)
                .containsExactly("brokers", "queue orders", "queue orders", "shard orders/0", "brokers"));
    }

    @Test
    void aClosedSubscriptionHearsNothingMore() {
        List<String> heard = new CopyOnWriteArrayList<>();
        Coordinator.Subscription subscription = coordinator.watch(new Coordinator.Listener() {
            @Override
            public void onBrokersChanged() {
                heard.add("brokers");
            }
        });
        coordinator.register(BROKER_1, () -> { });
        await().untilAsserted(() -> assertThat(heard).hasSize(1));

        subscription.close();
        coordinator.register(BROKER_2, () -> { });
        coordinator.createQueue(QueueConfig.builder("orders").build(), List.of(state("a", 0)));
        List<String> afterwards = new CopyOnWriteArrayList<>();
        coordinator.watch(new Coordinator.Listener() {
            @Override
            public void onQueueChanged(String queue) {
                afterwards.add(queue);
            }
        });
        coordinator.createQueue(QueueConfig.builder("later").build(), List.of(state("a", 0)));

        // The watch thread delivers in order, so once the later event has arrived the earlier ones have too.
        await().untilAsserted(() -> assertThat(afterwards).containsExactly("later"));
        assertThat(heard).hasSize(1);
    }

    private static ShardState state(String leader, int epoch) {
        ReplicaId replica = new ReplicaId(leader, "first");
        return new ShardState(replica, epoch, List.of(replica), List.of(leader));
    }
}
