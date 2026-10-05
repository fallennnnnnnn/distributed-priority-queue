package com.queue.distributedpriorityqueue.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.exception.BrokerUnreachableException;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.exception.ShardUnavailableException;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import com.queue.distributedpriorityqueue.testsupport.TestCluster;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** What happens when leaders die, are cut off, or come back. One test per row of the README's failure table. */
@Timeout(60)
class FailoverTest {

    private static final ShardId SHARD = new ShardId("orders", 0);
    private static final Duration SOON = Duration.ofSeconds(5);

    private TestCluster cluster;
    private String leader;

    @AfterEach
    void stop() {
        cluster.close();
    }

    @RepeatedTest(5)
    void anInSyncReplicaTakesOverWhenTheLeaderDies() {
        startWithReplicatedQueue(3, 3, 2, false);
        for (int i = 0; i < 10; i++) {
            enqueue(leader, "m" + i);
        }
        awaitInSync();
        ShardState before = state();

        cluster.crash(leader);
        String newLeader = awaitNewLeader();

        ShardState after = state();
        assertThat(after.epoch()).isEqualTo(before.epoch() + 1);
        assertThat(before.isr()).as("only an in-sync replica may take over").contains(after.leader());
        assertThat(after.isr()).doesNotContain(before.leader()).hasSize(2);
        assertThat(cluster.brokers()).filteredOn(broker -> broker.replica(SHARD).orElseThrow().isLeader())
                .as("exactly one broker leads").hasSize(1);
        String follower = cluster.followersOf(SHARD).get(0).info().id();
        await().until(() -> cluster.replica(follower, SHARD).epoch() == after.epoch());
        assertThat(cluster.queues(follower).enqueue("orders", "after", "LOW", null, null)).succeedsWithin(SOON);
        assertThat(drain(newLeader)).hasSize(11).contains("m0", "m9", "after");
        assertThat(cluster.replica(newLeader, SHARD).electionsWon()).isEqualTo(1);
        assertThat(cluster.replica(newLeader, SHARD).resets()).isZero();
    }

    @Test
    void aCleanShutdownHandsTheShardOverWithoutWaitingForTheLease() {
        startWithReplicatedQueue(3, 3, 2, false);
        enqueue(leader, "a");
        awaitInSync();

        cluster.stop(leader);

        String newLeader = awaitNewLeader();
        assertThat(state().epoch()).isEqualTo(1);
        assertThat(drain(newLeader)).containsExactly("a");
    }

    @Test
    void inFlightMessagesKeepTheirDeadlinesAndCountsAndAckedOnesStayGone() {
        startWithReplicatedQueue(3, 3, 2, false);
        enqueue(leader, "a");
        enqueue(leader, "b");
        enqueue(leader, "c");
        assertThat(dequeue(leader)).map(MessageView::payload).contains("a");
        MessageView b = dequeue(leader).orElseThrow();
        assertThat(cluster.queues(leader).ack("orders", b.id()).join()).isTrue();
        cluster.clock.advance(Duration.ofSeconds(10));
        awaitInSync();

        cluster.crash(leader);
        String newLeader = awaitNewLeader();

        assertThat(dequeue(newLeader)).map(MessageView::payload).contains("c");
        assertThat(dequeue(newLeader)).as("a is still in flight, b was acknowledged").isEmpty();
        cluster.clock.advance(Duration.ofSeconds(20).minusMillis(1));
        assertThat(dequeue(newLeader)).as("a's deadline was set by the old leader and has not passed").isEmpty();
        cluster.clock.advance(Duration.ofMillis(1));
        MessageView redelivered = dequeue(newLeader).orElseThrow();
        assertThat(redelivered.payload()).isEqualTo("a");
        assertThat(redelivered.deliveryCount()).isEqualTo(2);
        assertThat(dequeue(newLeader)).isEmpty();
        assertThat(cluster.queues(newLeader).metrics("orders").join().ackedTotal()).isEqualTo(1);
    }

    @Test
    void noAcknowledgedEnqueueIsLostWhenTheLeaderDiesUnderLoad() throws Exception {
        startWithReplicatedQueue(3, 3, 2, false);
        List<String> survivors = cluster.brokers().stream().map(broker -> broker.info().id())
                .filter(id -> !id.equals(leader)).toList();
        Set<String> acknowledged = ConcurrentHashMap.newKeySet();
        ExecutorService producers = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> running = new ArrayList<>();
            for (int producer = 0; producer < 4; producer++) {
                QueueService queues = cluster.queues(survivors.get(producer % survivors.size()));
                int id = producer;
                running.add(producers.submit(() -> {
                    for (int i = 0; i < 150; i++) {
                        String payload = "p" + id + "-" + i;
                        while (true) {
                            try {
                                queues.enqueue("orders", payload, "MEDIUM", null, null, Acks.ALL).get(5, TimeUnit.SECONDS);
                                acknowledged.add(payload);
                                break;
                            } catch (Exception shardBetweenLeaders) {
                                Thread.sleep(5);
                            }
                        }
                    }
                    return null;
                }));
            }
            await().until(() -> acknowledged.size() >= 150);
            cluster.crash(leader);
            for (Future<?> producer : running) {
                producer.get(40, TimeUnit.SECONDS);
            }
        } finally {
            producers.shutdownNow();
        }
        String newLeader = awaitNewLeader();

        assertThat(acknowledged).hasSize(600);
        assertThat(drain(newLeader)).as("a retried enqueue may appear twice, but none may be missing")
                .containsAll(acknowledged);
    }

    @Test
    void aLeaderCutOffFromEveryoneIsReplacedAndStepsDownWhenItReturns() {
        startWithReplicatedQueue(3, 3, 2, false);
        enqueue(leader, "committed");
        awaitInSync();
        String oldLeader = leader;

        cluster.network.isolate(oldLeader);
        CompletableFuture<MessageId> stranded =
                cluster.queues(oldLeader).enqueue("orders", "never committed", "HIGH", null, null);
        cluster.cutFromCoordinator(oldLeader);
        String newLeader = awaitNewLeader();

        // The old leader knows it has lost its lease and serves nothing.
        assertThat(cluster.broker(oldLeader).isFenced()).isTrue();
        assertThat(cluster.queues(oldLeader).enqueue("orders", "refused", "HIGH", null, null))
                .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(ShardUnavailableException.class);
        assertThat(stranded).as("its followers are gone, so it cannot commit").isNotDone();
        assertThat(cluster.replica(oldLeader, SHARD).status().logEnd()).isEqualTo(2);
        assertThat(cluster.replica(oldLeader, SHARD).status().highWatermark()).isEqualTo(1);
        enqueue(newLeader, "after");

        cluster.network.heal();
        cluster.restoreCoordinator(oldLeader);

        await().untilAsserted(() -> {
            ShardReplica returned = cluster.replica(oldLeader, SHARD);
            assertThat(cluster.broker(oldLeader).isFenced()).isFalse();
            assertThat(returned.isLeader()).isFalse();
            assertThat(returned.epoch()).isEqualTo(1);
            assertThat(returned.status().logEnd()).isEqualTo(cluster.replica(newLeader, SHARD).status().logEnd());
        });
        // The old leader dropped its uncommitted copy when it stepped down. The broker that took the
        // request then tried once more on the new leader, so the caller's enqueue succeeds after all.
        assertThat(stranded).succeedsWithin(SOON).isEqualTo(new MessageId(0, 2));
        cluster.broker(newLeader).maintainReplication();
        awaitInSync();
        assertThat(cluster.replica(oldLeader, SHARD).snapshot()).isEqualTo(cluster.replica(newLeader, SHARD).snapshot());
        assertThat(drain(oldLeader)).as("served through the old leader, which now passes requests on")
                .containsExactly("committed", "after", "never committed");
    }

    @Test
    void aRequestNamingAnotherEpochIsRefused() {
        startWithReplicatedQueue(3, 3, 2, false);
        BrokerApi api = cluster.broker(leader).api();

        assertThat(api.enqueue(SHARD, 1, "a", "HIGH", null, Acks.ALL))
                .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(NotLeaderException.class);
        assertThat(api.dequeue(SHARD, -1))
                .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(NotLeaderException.class);
        assertThat(api.enqueue(SHARD, 0, "a", "HIGH", null, Acks.ALL)).succeedsWithin(SOON);
    }

    @Test
    void aRestartedBrokerIsNotElectedBeforeItHasCaughtUp() throws Exception {
        String follower = startWithReplicatedQueue(2, 2, 1, false);
        enqueue(leader, "a");
        awaitInSync();
        ShardState before = state();

        cluster.network.hold(leader, follower);
        cluster.crash(follower);
        ReplicaId restarted = cluster.start(follower).info().replicaId();
        await().until(() -> cluster.broker(follower).replica(SHARD).isPresent());
        cluster.crash(leader);

        // The restarted broker has an empty log and is not in the in-sync set, so it must not take over.
        Thread.sleep(500);
        assertThat(state()).isEqualTo(before);
        assertThat(before.isr()).doesNotContain(restarted);
        assertThat(cluster.replica(follower, SHARD).isLeader()).isFalse();
        assertThat(cluster.queues(follower).enqueue("orders", "b", "HIGH", null, null))
                .failsWithin(SOON).withThrowableThat().havingCause().isInstanceOfAny(
                        ShardUnavailableException.class, BrokerUnreachableException.class);
    }

    @Test
    void whenEveryInSyncReplicaIsLostAnUncleanElectionBringsTheShardBackWithLoss() {
        String follower = startWithReplicatedQueue(2, 2, 1, true);
        enqueue(leader, "lost with the leader");
        awaitInSync();

        cluster.network.hold(leader, follower);
        cluster.crash(follower);
        ReplicaId restarted = cluster.start(follower).info().replicaId();
        await().until(() -> cluster.broker(follower).replica(SHARD).isPresent());
        cluster.crash(leader);

        await().until(() -> restarted.equals(state().leader()));
        assertThat(state().epoch()).isEqualTo(1);
        assertThat(state().isr()).containsExactly(restarted);
        await().until(() -> cluster.replica(follower, SHARD).isLeader());
        assertThat(cluster.replica(follower, SHARD).resets()).as("the loss is counted, not silent").isEqualTo(1);
        assertThat(drain(follower)).isEmpty();
        assertThat(cluster.queues(follower).enqueue("orders", "new", "HIGH", null, null)).succeedsWithin(SOON);
    }

    @Test
    void anUnreplicatedShardComesBackEmptyWhenItsBrokerRestarts() {
        cluster = new TestCluster(3);
        cluster.queues("broker-1").createQueue(QueueConfig.builder("orders").build());
        leader = cluster.leaderOf(SHARD).info().id();
        String other = cluster.brokers().stream().map(broker -> broker.info().id())
                .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        enqueue(other, "a");

        cluster.crash(leader);
        // Until the other brokers have heard that the leader is gone, they still try to reach it.
        assertThat(cluster.queues(other).enqueue("orders", "b", "HIGH", null, null))
                .failsWithin(SOON).withThrowableThat().havingCause().isInstanceOfAny(
                        ShardUnavailableException.class, BrokerUnreachableException.class);
        ReplicaId restarted = cluster.start(leader).info().replicaId();

        await().until(() -> restarted.equals(state().leader()));
        await().until(() -> cluster.replica(leader, SHARD).isLeader());
        assertThat(state().epoch()).isEqualTo(1);
        assertThat(cluster.replica(leader, SHARD).resets()).isEqualTo(1);
        await().untilAsserted(() ->
                assertThat(cluster.queues(other).enqueue("orders", "c", "HIGH", null, null)).succeedsWithin(SOON));
        assertThat(drain(other)).containsExactly("c");
    }

    @Test
    void brokersStopWithoutTheCoordinatorAndResumeWithEverythingIntact() {
        startWithReplicatedQueue(3, 3, 2, false);
        for (int i = 0; i < 10; i++) {
            enqueue(leader, "m" + i);
        }
        awaitInSync();
        List<String> everyone = cluster.brokers().stream().map(broker -> broker.info().id()).toList();

        everyone.forEach(cluster::cutFromCoordinator);
        await().until(() -> cluster.brokers().stream().allMatch(Broker::isFenced));
        assertThat(cluster.queues(leader).enqueue("orders", "refused", "HIGH", null, null))
                .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(ShardUnavailableException.class);

        everyone.forEach(cluster::restoreCoordinator);

        await().until(() -> cluster.brokers().stream().noneMatch(Broker::isFenced) && cluster.leaderOf(SHARD) != null
                && cluster.leaderOf(SHARD).replica(SHARD).orElseThrow().isLeader());
        String leaderNow = cluster.leaderOf(SHARD).info().id();
        await().untilAsserted(() ->
                assertThat(cluster.queues(leaderNow).enqueue("orders", "after", "LOW", null, null)).succeedsWithin(SOON));
        List<String> delivered = drain(leaderNow);
        assertThat(delivered).contains("m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "after");
        assertThat(delivered).doesNotContain("refused");
    }

    /**
     * Starts the brokers and creates a one-shard queue. Returns the ID of one follower, or null
     * when the shard has no followers.
     */
    private String startWithReplicatedQueue(int brokers, int replicationFactor, int minInSyncReplicas,
            boolean uncleanLeaderElection) {
        cluster = new TestCluster(brokers);
        cluster.queues("broker-1").createQueue(QueueConfig.builder("orders").replicationFactor(replicationFactor)
                .minInSyncReplicas(minInSyncReplicas).uncleanLeaderElection(uncleanLeaderElection).build());
        leader = cluster.leaderOf(SHARD).info().id();
        await().until(() -> cluster.followersOf(SHARD).size() == replicationFactor - 1);
        return cluster.followersOf(SHARD).stream().map(broker -> broker.info().id()).findFirst().orElse(null);
    }

    private ShardState state() {
        return cluster.coordinator.shard(SHARD).orElseThrow().value();
    }

    /** Waits for a running broker other than the old leader to lead the shard, and returns its ID. */
    private String awaitNewLeader() {
        String oldLeader = leader;
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            Broker now = cluster.leaderOf(SHARD);
            return now != null && !now.info().id().equals(oldLeader) && now.replica(SHARD).orElseThrow().isLeader();
        });
        leader = cluster.leaderOf(SHARD).info().id();
        return leader;
    }

    /** Waits until every running replica has the leader's whole log and knows it is committed. */
    private void awaitInSync() {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ShardReplica.Status onLeader = cluster.leaderOf(SHARD).replica(SHARD).orElseThrow().status();
            assertThat(onLeader.highWatermark()).isEqualTo(onLeader.logEnd());
            for (Broker follower : cluster.followersOf(SHARD)) {
                ShardReplica.Status onFollower = follower.replica(SHARD).orElseThrow().status();
                assertThat(onFollower.logEnd()).isEqualTo(onLeader.logEnd());
                assertThat(onFollower.highWatermark()).isEqualTo(onLeader.logEnd());
            }
        });
    }

    private MessageId enqueue(String broker, String payload) {
        return cluster.queues(broker).enqueue("orders", payload, "HIGH", null, null).join();
    }

    private Optional<MessageView> dequeue(String broker) {
        return cluster.queues(broker).dequeue("orders", Duration.ZERO).join();
    }

    private List<String> drain(String broker) {
        List<String> payloads = new ArrayList<>();
        for (Optional<MessageView> next = dequeue(broker); next.isPresent(); next = dequeue(broker)) {
            payloads.add(next.get().payload());
        }
        return payloads;
    }
}
