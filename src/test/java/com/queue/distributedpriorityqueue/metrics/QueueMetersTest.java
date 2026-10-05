package com.queue.distributedpriorityqueue.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queue.distributedpriorityqueue.cluster.Broker;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.testsupport.TestCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** A shard's meters are published by whichever broker leads it, and by no other. */
@Timeout(60)
class QueueMetersTest {

    private static final ShardId SHARD = new ShardId("orders", 0);

    private final TestCluster cluster = new TestCluster(3);
    private final Map<String, MeterRegistry> registries = new HashMap<>();

    @BeforeEach
    void publishMeters() {
        for (Broker broker : cluster.brokers()) {
            MeterRegistry registry = new SimpleMeterRegistry();
            registries.put(broker.info().id(), registry);
            new QueueMeters(registry, broker);
        }
        cluster.queues("broker-1").createQueue(
                QueueConfig.builder("orders").replicationFactor(3).minInSyncReplicas(2).build());
        await().until(() -> cluster.followersOf(SHARD).size() == 2);
    }

    @AfterEach
    void stop() {
        cluster.close();
    }

    @Test
    void onlyTheLeaderPublishesAShardsMeters() {
        String leader = cluster.leaderOf(SHARD).info().id();
        cluster.queues(leader).enqueue("orders", "a", "HIGH", null, null).join();
        cluster.queues(leader).enqueue("orders", "b", "LOW", null, null).join();

        MeterRegistry onLeader = registries.get(leader);
        assertThat(onLeader.get("dpq.messages.ready").tag("queue", "orders").tag("shard", "0").tag("priority", "HIGH")
                .gauge().value()).isEqualTo(1);
        assertThat(onLeader.get("dpq.messages.enqueued").functionCounter().count()).isEqualTo(2);
        assertThat(onLeader.get("dpq.in.sync.replicas").gauge().value()).isEqualTo(3);
        assertThat(onLeader.get("dpq.under.replicated").gauge().value()).isZero();
        assertThat(onLeader.get("dpq.leader.epoch").gauge().value()).isZero();
        assertThat(onLeader.get("dpq.log.end.offset").gauge().value()).isEqualTo(2);
        assertThat(onLeader.get("dpq.high.watermark").gauge().value()).isEqualTo(2);
        assertThat(onLeader.get("dpq.leader.elections").functionCounter().count()).isZero();
        assertThat(onLeader.get("dpq.shard.resets").functionCounter().count()).isZero();
        for (Broker follower : cluster.followersOf(SHARD)) {
            assertThat(registries.get(follower.info().id()).find("dpq.messages.ready").gauges()).isEmpty();
        }
    }

    @Test
    void theMetersMoveToTheNewLeaderAfterAFailover() {
        String oldLeader = cluster.leaderOf(SHARD).info().id();
        cluster.queues(oldLeader).enqueue("orders", "a", "HIGH", null, null).join();
        await().until(() -> cluster.followersOf(SHARD).stream()
                .allMatch(follower -> follower.replica(SHARD).orElseThrow().status().highWatermark() == 1));

        cluster.crash(oldLeader);
        await().until(() -> cluster.leaderOf(SHARD) != null
                && cluster.leaderOf(SHARD).replica(SHARD).orElseThrow().isLeader());
        String newLeader = cluster.leaderOf(SHARD).info().id();

        MeterRegistry onNewLeader = registries.get(newLeader);
        await().untilAsserted(() -> assertThat(onNewLeader.find("dpq.messages.ready").gauges()).hasSize(3));
        assertThat(onNewLeader.get("dpq.messages.enqueued").functionCounter().count())
                .as("the new leader replayed the same events, so its totals carry on").isEqualTo(1);
        assertThat(onNewLeader.get("dpq.leader.epoch").gauge().value()).isEqualTo(1);
        assertThat(onNewLeader.get("dpq.leader.elections").functionCounter().count()).isEqualTo(1);
        assertThat(onNewLeader.get("dpq.in.sync.replicas").gauge().value()).isEqualTo(2);
        assertThat(onNewLeader.get("dpq.under.replicated").gauge().value()).isEqualTo(1);
        String follower = cluster.followersOf(SHARD).get(0).info().id();
        assertThat(registries.get(follower).find("dpq.messages.ready").gauges()).isEmpty();
    }

    @Test
    void aLeaderThatStepsDownWithdrawsItsMeters() {
        String oldLeader = cluster.leaderOf(SHARD).info().id();
        MeterRegistry onOldLeader = registries.get(oldLeader);
        assertThat(onOldLeader.find("dpq.messages.ready").gauges()).hasSize(3);

        cluster.network.isolate(oldLeader);
        cluster.cutFromCoordinator(oldLeader);
        await().until(() -> cluster.leaderOf(SHARD) != null && !cluster.leaderOf(SHARD).info().id().equals(oldLeader));
        cluster.network.heal();
        cluster.restoreCoordinator(oldLeader);

        await().untilAsserted(() -> assertThat(onOldLeader.find("dpq.messages.ready").gauges()).isEmpty());
        assertThat(onOldLeader.find("dpq.in.sync.replicas").gauges()).isEmpty();
        assertThat(onOldLeader.find("dpq.messages.enqueued").functionCounters()).isEmpty();
    }
}
