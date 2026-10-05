package com.queue.distributedpriorityqueue.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.exception.CommitTimeoutException;
import com.queue.distributedpriorityqueue.exception.NotEnoughReplicasException;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import com.queue.distributedpriorityqueue.testsupport.TestCluster;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Several brokers in one JVM, with a fixed leader per shard. Followers are held back, cut off and
 * restarted; the leader never changes, which is the subject of the failover tests.
 */
@Timeout(60)
class ReplicationTest {

    private static final ShardId SHARD = new ShardId("orders", 0);
    private static final Duration PAST_LAG_MAX = Duration.ofSeconds(6);
    private static final Duration SOON = Duration.ofSeconds(5);

    private TestCluster cluster;
    private String leader;
    private String follower1;
    private String follower2;

    @AfterEach
    void stop() {
        cluster.close();
    }

    @Nested
    class Committing {

        @Test
        void anEnqueueIsAnsweredOnlyWhenEveryInSyncReplicaHasIt() throws Exception {
            startWithReplicatedQueue(3, 2);
            cluster.network.hold(leader, follower1);
            cluster.network.hold(leader, follower2);

            CompletableFuture<MessageId> enqueued = cluster.queues(leader).enqueue("orders", "a", "HIGH", null, null);
            Thread.sleep(150);
            assertThat(enqueued).isNotDone();

            cluster.network.release(leader, follower1);
            await().until(() -> status(follower1).logEnd() == 1);
            Thread.sleep(150);
            assertThat(enqueued).as("one in-sync replica still lacks the message").isNotDone();

            cluster.network.release(leader, follower2);
            assertThat(enqueued).succeedsWithin(SOON);
            assertThat(status(follower2).logEnd()).isEqualTo(1);
        }

        @Test
        void followersEndUpWithTheLeadersState() {
            startWithReplicatedQueue(3, 2);
            List<MessageId> ids = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                String broker = "broker-" + (i % 3 + 1);
                ids.add(cluster.queues(broker).enqueue("orders", "m" + i, i % 3 == 0 ? "HIGH" : "LOW", null, null).join());
            }
            for (int i = 0; i < 30; i++) {
                assertThat(cluster.queues("broker-" + (i % 3 + 1)).dequeue("orders", Duration.ZERO).join()).isPresent();
            }
            for (int i = 0; i < 10; i++) {
                assertThat(cluster.queues(follower1).ack("orders", ids.get(i * 3)).join()).isTrue();
            }

            awaitInSync(SHARD);

            ShardReplica.Snapshot onLeader = cluster.replica(leader, SHARD).snapshot();
            assertThat(onLeader.liveMessages()).hasSize(50);
            assertThat(cluster.replica(follower1, SHARD).snapshot()).isEqualTo(onLeader);
            assertThat(cluster.replica(follower2, SHARD).snapshot()).isEqualTo(onLeader);
        }

        @Test
        void acksLeaderIsAnsweredBeforeTheFollowersHaveTheMessage() {
            startWithReplicatedQueue(3, 2);
            cluster.network.hold(leader, follower1);
            cluster.network.hold(leader, follower2);

            CompletableFuture<MessageId> enqueued =
                    cluster.queues(follower1).enqueue("orders", "a", "HIGH", null, null, Acks.LEADER);

            assertThat(enqueued).succeedsWithin(SOON);
            assertThat(status(follower1).logEnd()).isZero();
            assertThat(status(follower2).logEnd()).isZero();
            cluster.network.release(leader, follower1);
            cluster.network.release(leader, follower2);
            awaitInSync(SHARD);
        }

        @Test
        void aWriteThatIsNotReplicatedInTimeFailsButIsNotLost() {
            cluster = new TestCluster(3, Duration.ofMillis(300));
            createReplicatedQueue(3, 2);
            cluster.network.hold(leader, follower1);
            cluster.network.hold(leader, follower2);

            CompletableFuture<MessageId> enqueued = cluster.queues(leader).enqueue("orders", "a", "HIGH", null, null);

            assertThat(enqueued).failsWithin(SOON).withThrowableThat().withCauseInstanceOf(CommitTimeoutException.class);
            cluster.network.release(leader, follower1);
            cluster.network.release(leader, follower2);
            awaitInSync(SHARD);
            assertThat(dequeue(leader)).map(MessageView::payload).contains("a");
            assertThat(dequeue(leader)).isEmpty();
        }

        @Test
        void aQueueWithDefaultSettingsIsNotReplicated() {
            cluster = new TestCluster(3);
            QueueMetadata queue = cluster.queues("broker-1").createQueue(QueueConfig.builder("orders").build());

            assertThat(queue.shard(0).replicas()).hasSize(1);
            assertThat(queue.shard(0).isr()).hasSize(1);
            for (String broker : List.of("broker-1", "broker-2", "broker-3")) {
                assertThat(cluster.queues(broker).enqueue("orders", "from " + broker, null, null, null))
                        .succeedsWithin(SOON);
            }
            assertThat(cluster.brokers()).filteredOn(broker -> broker.replica(SHARD).isPresent()).hasSize(1);
            assertThat(cluster.queues("broker-2").metrics("orders").join().enqueuedTotal()).isEqualTo(3);
        }
    }

    @Nested
    class InSyncSet {

        @Test
        void aFollowerThatFallsBehindIsDroppedAndWritesGoOn() throws Exception {
            startWithReplicatedQueue(3, 2);
            cluster.network.hold(leader, follower2);
            CompletableFuture<MessageId> enqueued = cluster.queues(leader).enqueue("orders", "a", "HIGH", null, null);
            awaitConfirmedBy(follower1);
            Thread.sleep(150);
            assertThat(enqueued).isNotDone();

            cluster.clock.advance(PAST_LAG_MAX);
            cluster.broker(leader).maintainReplication();

            assertThat(enqueued).succeedsWithin(SOON);
            await().untilAsserted(() -> assertThat(inSyncBrokers("broker-1")).containsExactlyInAnyOrder(leader, follower1));
            assertThat(cluster.queues(follower1).enqueue("orders", "b", "HIGH", null, null)).succeedsWithin(SOON);
        }

        @Test
        void aFollowerThatCatchesUpAgainIsAddedBack() {
            startWithReplicatedQueue(3, 2);
            cluster.network.hold(leader, follower2);
            cluster.queues(leader).enqueue("orders", "a", "HIGH", null, null);
            awaitConfirmedBy(follower1);
            cluster.clock.advance(PAST_LAG_MAX);
            cluster.broker(leader).maintainReplication();
            await().until(() -> status(leader).isr().size() == 2);

            cluster.network.release(leader, follower2);
            await().until(() -> status(follower2).logEnd() == status(leader).logEnd());
            cluster.broker(leader).maintainReplication();

            await().untilAsserted(() -> assertThat(inSyncBrokers("broker-3"))
                    .containsExactlyInAnyOrder(leader, follower1, follower2));
            awaitInSync(SHARD);
            assertThat(cluster.replica(follower2, SHARD).snapshot()).isEqualTo(cluster.replica(leader, SHARD).snapshot());
        }

        @Test
        void belowTheMinimumEnqueuesAreRefusedButConsumersCanDrain() {
            cluster = new TestCluster(2);
            createReplicatedQueue(2, 2);
            MessageId first = cluster.queues(leader).enqueue("orders", "a", "HIGH", null, null).join();
            awaitInSync(SHARD);
            cluster.network.hold(leader, follower1);
            cluster.queues(leader).enqueue("orders", "b", "HIGH", null, null, Acks.LEADER).join();
            cluster.clock.advance(PAST_LAG_MAX);
            cluster.broker(leader).maintainReplication();
            await().until(() -> status(leader).isr().size() == 1);

            assertThat(cluster.queues(follower1).enqueue("orders", "c", "HIGH", null, null))
                    .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(NotEnoughReplicasException.class);
            assertThat(cluster.queues(follower1).enqueue("orders", "d", "HIGH", null, null, Acks.LEADER))
                    .succeedsWithin(SOON);
            assertThat(dequeue(follower1)).map(MessageView::payload).contains("a");
            assertThat(cluster.queues(follower1).ack("orders", first).join()).isTrue();
        }

        @Test
        void aRestartedFollowerCatchesUpFromAnEmptyLog() {
            startWithReplicatedQueue(3, 2);
            for (int i = 0; i < 20; i++) {
                cluster.queues(leader).enqueue("orders", "before-" + i, "HIGH", null, null).join();
            }
            ReplicaId oldIncarnation = cluster.broker(follower2).info().replicaId();
            cluster.stop(follower2);
            cluster.clock.advance(PAST_LAG_MAX);
            cluster.broker(leader).maintainReplication();
            await().until(() -> !status(leader).isr().contains(oldIncarnation));
            for (int i = 0; i < 10; i++) {
                cluster.queues(leader).enqueue("orders", "during-" + i, "LOW", null, null).join();
            }

            ReplicaId newIncarnation = cluster.start(follower2).info().replicaId();

            assertThat(newIncarnation).isNotEqualTo(oldIncarnation);
            await().until(() -> cluster.broker(follower2).replica(SHARD).isPresent()
                    && status(follower2).logEnd() == 30);
            cluster.broker(leader).maintainReplication();
            await().untilAsserted(() -> assertThat(status(leader).isr())
                    .contains(newIncarnation).doesNotContain(oldIncarnation).hasSize(3));
            awaitInSync(SHARD);
            assertThat(cluster.replica(follower2, SHARD).snapshot()).isEqualTo(cluster.replica(leader, SHARD).snapshot());
        }

        @Test
        void aFollowerReconnectsWhenAPartitionHeals() {
            startWithReplicatedQueue(3, 2);
            cluster.queues(leader).enqueue("orders", "before", "HIGH", null, null).join();
            awaitInSync(SHARD);

            cluster.network.partition(leader, follower2);
            CompletableFuture<MessageId> during = cluster.queues(leader).enqueue("orders", "during", "HIGH", null, null);
            awaitConfirmedBy(follower1);
            cluster.clock.advance(PAST_LAG_MAX);
            cluster.broker(leader).maintainReplication();
            assertThat(during).succeedsWithin(SOON);
            assertThat(status(follower2).logEnd()).isEqualTo(1);

            cluster.network.heal();

            await().until(() -> status(follower2).logEnd() == 2);
            cluster.broker(leader).maintainReplication();
            await().until(() -> status(leader).isr().size() == 3);
            awaitInSync(SHARD);
            assertThat(cluster.replica(follower2, SHARD).snapshot()).isEqualTo(cluster.replica(leader, SHARD).snapshot());
        }
    }

    @Nested
    class AcrossBrokers {

        @Test
        void anyBrokerServesAnyRequest() {
            startWithQueueOfThreeUnreplicatedShards(3);
            List<MessageId> ids = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                ids.add(cluster.queues("broker-1").enqueue("orders", "m" + i, "MEDIUM", null, null).join());
            }
            assertThat(ids).extracting(MessageId::shard).containsOnly(0, 1, 2);

            Set<String> delivered = new HashSet<>();
            for (Optional<MessageView> next = dequeue("broker-2"); next.isPresent(); next = dequeue("broker-2")) {
                delivered.add(next.get().payload());
            }
            assertThat(delivered).hasSize(30);
            assertThat(ids).allSatisfy(id -> assertThat(cluster.queues("broker-3").ack("orders", id).join()).isTrue());

            for (String broker : List.of("broker-1", "broker-2", "broker-3")) {
                QueueMetrics metrics = cluster.queues(broker).metrics("orders").join();
                assertThat(metrics.enqueuedTotal()).isEqualTo(30);
                assertThat(metrics.ackedTotal()).isEqualTo(30);
                assertThat(metrics.readyTotal()).isZero();
                assertThat(metrics.inFlight()).isZero();
            }
        }

        @Test
        void aBrokerRefusesARequestForAShardItDoesNotLead() {
            startWithReplicatedQueue(3, 2);

            assertThat(cluster.broker(follower1).api().enqueue(SHARD, 0, "a", "HIGH", null, Acks.ALL))
                    .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(NotLeaderException.class);
            assertThat(cluster.broker(follower1).api().dequeue(SHARD, 0))
                    .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(NotLeaderException.class);
            assertThat(cluster.broker(follower2).api().dequeue(new ShardId("no-such-queue", 0), 0))
                    .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(NotLeaderException.class);
        }

        @Test
        void everyBrokerDescribesTheQueueTheSameWay() {
            startWithReplicatedQueue(3, 2);

            QueueMetadata onLeader = cluster.queues(leader).describe("orders");

            assertThat(onLeader.shard(0).leader().brokerId()).isEqualTo(leader);
            assertThat(onLeader.shard(0).replicas()).containsExactlyInAnyOrder("broker-1", "broker-2", "broker-3");
            assertThat(cluster.queues(follower1).describe("orders")).isEqualTo(onLeader);
            assertThat(cluster.queues(follower2).describe("orders")).isEqualTo(onLeader);
        }

        @Test
        void aDequeueGoesToTheShardWithTheHighestPriorityOnAnotherBroker() {
            startWithQueueOfThreeUnreplicatedShards(3);
            for (int i = 0; i < 6; i++) {
                cluster.queues("broker-1").enqueue("orders", "low-" + i, "LOW", null, null).join();
            }
            MessageId high = cluster.queues("broker-1").enqueue("orders", "high", "HIGH", null, "the-key").join();
            String leaderOfHigh = cluster.leaderOf(new ShardId("orders", high.shard())).info().id();
            String consumer = List.of("broker-1", "broker-2", "broker-3").stream()
                    .filter(broker -> !broker.equals(leaderOfHigh)).findFirst().orElseThrow();

            cluster.broker(consumer).exchangeSummaries().join();

            assertThat(dequeue(consumer)).map(MessageView::payload).contains("high");
        }

        @Test
        void metricsAndDeadLettersAreGatheredFromEveryLeader() {
            startWithQueueOfThreeUnreplicatedShards(0);
            for (int i = 0; i < 6; i++) {
                cluster.queues("broker-1").enqueue("orders", "poison-" + i, "HIGH", null, null).join();
            }
            for (int i = 0; i < 6; i++) {
                assertThat(dequeue("broker-1")).isPresent();
            }

            cluster.clock.advance(Duration.ofSeconds(30));
            assertThat(dequeue("broker-2")).isEmpty();

            assertThat(cluster.queues("broker-3").deadLetters("orders").join()).extracting(MessageView::payload)
                    .containsExactlyInAnyOrder("poison-0", "poison-1", "poison-2", "poison-3", "poison-4", "poison-5");
            QueueMetrics metrics = cluster.queues("broker-1").metrics("orders").join();
            assertThat(metrics.deadLetteredTotal()).isEqualTo(6);
            assertThat(metrics.deadLetters()).isEqualTo(6);
            assertThat(metrics.enqueuedTotal()).isEqualTo(6);
        }
    }

    private void startWithReplicatedQueue(int replicationFactor, int minInSyncReplicas) {
        cluster = new TestCluster(3);
        createReplicatedQueue(replicationFactor, minInSyncReplicas);
    }

    /** One shard, replicated. Waits until every replica exists and has opened its fetch stream. */
    private void createReplicatedQueue(int replicationFactor, int minInSyncReplicas) {
        cluster.queues("broker-1").createQueue(QueueConfig.builder("orders")
                .replicationFactor(replicationFactor).minInSyncReplicas(minInSyncReplicas).build());
        leader = cluster.leaderOf(SHARD).info().id();
        await().until(() -> cluster.followersOf(SHARD).size() == replicationFactor - 1);
        List<Broker> followers = cluster.followersOf(SHARD);
        follower1 = followers.get(0).info().id();
        follower2 = followers.size() > 1 ? followers.get(1).info().id() : null;
        // An event that every follower must fetch proves that all the streams are open.
        cluster.queues(leader).updateQueue("orders", config -> config);
        cluster.broker(leader).sweep();
        awaitInSync(SHARD);
    }

    /** Three shards with one replica each, so that every broker leads one of them. */
    private void startWithQueueOfThreeUnreplicatedShards(int maxRetries) {
        cluster = new TestCluster(3);
        cluster.queues("broker-1").createQueue(QueueConfig.builder("orders").shards(3).maxRetries(maxRetries).build());
        Set<String> leaders = new HashSet<>();
        for (int shard = 0; shard < 3; shard++) {
            leaders.add(cluster.leaderOf(new ShardId("orders", shard)).info().id());
        }
        assertThat(leaders).containsExactlyInAnyOrder("broker-1", "broker-2", "broker-3");
    }

    private void awaitInSync(ShardId shard) {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ShardReplica.Status onLeader = cluster.leaderOf(shard).replica(shard).orElseThrow().status();
            assertThat(onLeader.highWatermark()).isEqualTo(onLeader.logEnd());
            for (Broker follower : cluster.followersOf(shard)) {
                ShardReplica.Status onFollower = follower.replica(shard).orElseThrow().status();
                assertThat(onFollower.logEnd()).isEqualTo(onLeader.logEnd());
                assertThat(onFollower.highWatermark()).isEqualTo(onLeader.logEnd());
            }
        });
    }

    /**
     * Waits until the leader knows that the follower has its whole log. Tests do this before
     * jumping the clock past replicaLagMax, so that a confirmation still on its way is not mistaken
     * for a follower that fell behind.
     */
    private void awaitConfirmedBy(String follower) {
        ReplicaId replica = cluster.broker(follower).info().replicaId();
        await().until(() -> {
            ShardReplica.Status onLeader = status(leader);
            return onLeader.followerLogEnds().getOrDefault(replica, -1L) == onLeader.logEnd();
        });
    }

    private ShardReplica.Status status(String broker) {
        return cluster.replica(broker, SHARD).status();
    }

    private Optional<MessageView> dequeue(String broker) {
        return cluster.queues(broker).dequeue("orders", Duration.ZERO).join();
    }

    /** The in-sync brokers of the shard, as the given broker describes them. */
    private List<String> inSyncBrokers(String describingBroker) {
        return cluster.queues(describingBroker).describe("orders").shard(0).isr().stream()
                .map(ReplicaId::brokerId).toList();
    }
}
