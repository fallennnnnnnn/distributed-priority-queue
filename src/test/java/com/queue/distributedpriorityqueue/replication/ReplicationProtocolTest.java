package com.queue.distributedpriorityqueue.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.coordination.ShardState;
import com.queue.distributedpriorityqueue.coordination.Versioned;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.core.ShardEvent;
import com.queue.distributedpriorityqueue.exception.CommitTimeoutException;
import com.queue.distributedpriorityqueue.exception.NotEnoughReplicasException;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.log.InMemoryShardLog;
import com.queue.distributedpriorityqueue.log.LogEntry;
import com.queue.distributedpriorityqueue.log.ShardLog.EpochEnd;
import com.queue.distributedpriorityqueue.replication.ShardReplica.IsrProposal;
import com.queue.distributedpriorityqueue.testsupport.ManualClock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The replication rules of one shard, with a leader replica and a follower replica wired directly
 * to each other: no brokers, no network and no background threads.
 */
class ReplicationProtocolTest {

    private static final ShardId SHARD = new ShardId("orders", 0);
    private static final ReplicaId LEADER = new ReplicaId("broker-1", "a");
    private static final ReplicaId FOLLOWER = new ReplicaId("broker-2", "a");
    private static final Duration LAG_MAX = Duration.ofSeconds(5);

    private final ManualClock clock = new ManualClock();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private QueueConfig config = QueueConfig.builder("orders").replicationFactor(2).build();

    @AfterEach
    void stop() {
        scheduler.shutdownNow();
    }

    @Nested
    class OpeningAStream {

        @Test
        void sendsTheLogFromTheFollowersOffsetOn() {
            ShardReplica leader = leader(log(1, 1, 1), 1);
            Recorder follower = new Recorder();

            leader.openFetch(new FetchRequest(SHARD, FOLLOWER, 1, 1, 1), follower);

            assertThat(follower.entries).extracting(LogEntry::offset).containsExactly(1L, 2L);
            assertThat(follower.ends).isEmpty();
        }

        @Test
        void aFollowerFromAnOlderEpochIsFenced() {
            ShardReplica leader = leader(log(1, 1), 3);
            Recorder follower = new Recorder();

            leader.openFetch(new FetchRequest(SHARD, FOLLOWER, 2, 2, 1), follower);

            assertThat(follower.ends).containsExactly(FetchEnd.of(FetchEnd.Reason.FENCED_EPOCH));
            assertThat(follower.entries).isEmpty();
        }

        @Test
        void aReplicaThatDoesNotLeadRefuses() {
            ShardReplica notLeader = follower(log(), 1);
            Recorder asking = new Recorder();

            notLeader.openFetch(new FetchRequest(SHARD, LEADER, 1, 0, -1), asking);

            assertThat(asking.ends).containsExactly(FetchEnd.of(FetchEnd.Reason.NOT_LEADER));
        }

        @Test
        void aLeaderBehindTheFollowersEpochRefuses() {
            ShardReplica staleLeader = leader(log(1), 1);
            Recorder follower = new Recorder();

            staleLeader.openFetch(new FetchRequest(SHARD, FOLLOWER, 2, 1, 1), follower);

            assertThat(follower.ends).containsExactly(FetchEnd.of(FetchEnd.Reason.NOT_LEADER));
        }

        @Test
        void aBrokerThatHoldsNoReplicaIsTurnedAway() {
            ShardReplica leader = leader(log(1), 1);
            Recorder stranger = new Recorder();

            leader.openFetch(new FetchRequest(SHARD, new ReplicaId("broker-9", "a"), 1, 0, -1), stranger);

            assertThat(stranger.ends).containsExactly(FetchEnd.of(FetchEnd.Reason.CLOSED));
        }
    }

    @Nested
    class Divergence {

        @Test
        void aFollowerWhoseLogRunsPastTheLeadersIsToldWhereTheEpochEnds() {
            ShardReplica leader = leader(log(1, 1, 1, 1, 1), 2);
            Recorder follower = new Recorder();

            leader.openFetch(new FetchRequest(SHARD, FOLLOWER, 2, 8, 1), follower);

            assertThat(follower.ends).containsExactly(FetchEnd.diverged(new EpochEnd(1, 5)));
        }

        @Test
        void aFollowerHoldingEntriesFromAnAbandonedEpochCutsThemOff() {
            InMemoryShardLog followerLog = log(1, 1, 1, 1, 1, 2, 2);
            ShardReplica leader = leader(log(1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 3);
            ShardReplica follower = follower(followerLog, 3);
            Recorder recorder = new Recorder();

            leader.openFetch(follower.fetchRequest(), recorder);
            assertThat(recorder.ends).containsExactly(FetchEnd.diverged(new EpochEnd(1, 10)));
            follower.truncateToAgreeWith(recorder.ends.get(0).leaderEpochEnd());

            // The leader's epoch 1 runs to offset 10, but the follower's own epoch 1 ended at 5.
            assertThat(followerLog.endOffset()).isEqualTo(5);
            assertThat(follower.fetchRequest()).isEqualTo(new FetchRequest(SHARD, FOLLOWER, 3, 5, 1));
        }

        @Test
        void aFollowerWithNothingInCommonStartsOver() {
            InMemoryShardLog followerLog = log(1, 1);
            ShardReplica leader = leader(log(2, 2, 2), 2);
            ShardReplica follower = follower(followerLog, 2);
            Recorder recorder = new Recorder();

            leader.openFetch(follower.fetchRequest(), recorder);
            follower.truncateToAgreeWith(recorder.ends.get(0).leaderEpochEnd());

            assertThat(recorder.ends).containsExactly(FetchEnd.diverged(new EpochEnd(-1, 0)));
            assertThat(followerLog.endOffset()).isZero();
        }

        @Test
        void aDivergedFollowerEndsUpWithTheLeadersLogAndState() {
            InMemoryShardLog leaderLog = log(1, 1, 1, 1, 1, 3, 3, 3);
            InMemoryShardLog followerLog = log(1, 1, 1, 1, 1, 1, 2, 2, 2, 2);
            ShardReplica leader = leader(leaderLog, 3);
            ShardReplica follower = follower(followerLog, 3);

            connect(follower, leader).start();

            assertThat(followerLog.read(0, 100)).isEqualTo(leaderLog.read(0, 100));
            assertThat(follower.status().highWatermark()).isEqualTo(8);
            assertThat(follower.snapshot()).isEqualTo(leader.snapshot());
        }
    }

    @Nested
    class Committing {

        @Test
        void aWriteIsCommittedOnceTheFollowerReportsHavingIt() {
            ShardReplica leader = leader(log(), 1);
            Recorder follower = new Recorder();
            FetchStream stream = leader.openFetch(new FetchRequest(SHARD, FOLLOWER, 1, 0, -1), follower);

            CompletableFuture<MessageId> enqueued = leader.enqueue("a", "HIGH", null);
            assertThat(enqueued).isNotDone();
            assertThat(follower.entries).hasSize(1);
            assertThat(leader.status().highWatermark()).isZero();

            stream.reportLogEnd(1);

            assertThat(enqueued).isCompletedWithValue(new MessageId(0, 0));
            assertThat(leader.status().highWatermark()).isEqualTo(1);
            assertThat(follower.highWatermarks).last().isEqualTo(1L);
        }

        @Test
        void theNextBatchWaitsForTheLastOneToBeConfirmed() {
            ShardReplica leader = leader(log(), 1);
            Recorder follower = new Recorder();
            FetchStream stream = leader.openFetch(new FetchRequest(SHARD, FOLLOWER, 1, 0, -1), follower);

            leader.enqueue("a", "HIGH", null);
            leader.enqueue("b", "HIGH", null);
            leader.enqueue("c", "HIGH", null);
            assertThat(follower.entries).hasSize(1);

            stream.reportLogEnd(1);

            assertThat(follower.entries).extracting(LogEntry::offset).containsExactly(0L, 1L, 2L);
        }

        @Test
        void aFollowerAppliesOnlyWhatIsCommitted() {
            ShardReplica leader = leader(log(), 1);
            ShardReplica follower = follower(log(), 1);
            Recorder recorder = new Recorder();
            FetchStream stream = leader.openFetch(follower.fetchRequest(), recorder);
            leader.enqueue("a", "HIGH", null);

            follower.appendFetched(recorder.entries, 0);
            assertThat(follower.status().logEnd()).isEqualTo(1);
            assertThat(follower.snapshot().liveMessages()).isEmpty();

            stream.reportLogEnd(1);
            follower.appendFetched(List.of(), recorder.highWatermarks.get(recorder.highWatermarks.size() - 1));

            assertThat(follower.snapshot().liveMessages()).hasSize(1);
            assertThat(follower.snapshot()).isEqualTo(leader.snapshot());
        }

        @Test
        void aFollowerKeepsUpThroughAWorkload() {
            ShardReplica leader = leader(log(), 1);
            ShardReplica follower = follower(log(), 1);
            connect(follower, leader).start();

            List<MessageId> ids = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                ids.add(leader.enqueue("m" + i, i % 2 == 0 ? "HIGH" : "LOW", null).join());
            }
            for (int i = 0; i < 20; i++) {
                leader.dequeue().join();
            }
            for (int i = 0; i < 10; i++) {
                assertThat(leader.ack(ids.get(i * 2)).join()).isTrue();
            }

            assertThat(follower.status().logEnd()).isEqualTo(leader.status().logEnd()).isEqualTo(80);
            assertThat(follower.status().highWatermark()).isEqualTo(80);
            assertThat(follower.snapshot()).isEqualTo(leader.snapshot());
        }

        @Test
        void aWriteFailsWhenTheFollowersDoNotConfirmInTime() {
            ShardReplica leader = new ShardReplica(SHARD, LEADER, config, log(), clock, LAG_MAX, Duration.ofMillis(100));
            leader.lead(state(1));

            CompletableFuture<MessageId> enqueued = leader.enqueue("a", "HIGH", null);

            assertThat(enqueued).failsWithin(5, TimeUnit.SECONDS).withThrowableThat()
                    .withCauseInstanceOf(CommitTimeoutException.class);
            assertThat(leader.status().logEnd()).as("the write stays in the log").isEqualTo(1);
        }

        @Test
        void acksLeaderDoesNotWaitForFollowers() {
            ShardReplica leader = leader(log(), 1);

            assertThat(leader.enqueue("a", "HIGH", null, Acks.LEADER)).isCompletedWithValue(new MessageId(0, 0));
            assertThat(leader.status().highWatermark()).isZero();
        }

        @Test
        void aFollowerServesNoRequests() {
            ShardReplica follower = follower(log(), 1);

            assertThatExceptionOfType(NotLeaderException.class).isThrownBy(() -> follower.enqueue("a", "HIGH", null));
            assertThatExceptionOfType(NotLeaderException.class).isThrownBy(follower::dequeue);
            assertThatExceptionOfType(NotLeaderException.class).isThrownBy(() -> follower.ack(new MessageId(0, 0)));
            assertThatExceptionOfType(NotLeaderException.class).isThrownBy(follower::metrics);
            assertThatExceptionOfType(NotLeaderException.class).isThrownBy(follower::deadLetters);
        }
    }

    @Nested
    class InSyncSet {

        @Test
        void isLeftAloneWhileTheFollowerKeepsUp() {
            ShardReplica leader = leader(log(), 1);
            connect(follower(log(), 1), leader).start();
            leader.enqueue("a", "HIGH", null).join();

            clock.advance(LAG_MAX.multipliedBy(10));

            assertThat(leader.proposeIsrChange()).isEmpty();
        }

        @Test
        void aFollowerThatStaysBehindIsDroppedAndWritesGoThrough() {
            ShardReplica leader = leader(log(), 1);
            CompletableFuture<MessageId> enqueued = leader.enqueue("a", "HIGH", null);

            clock.advance(LAG_MAX);
            assertThat(leader.proposeIsrChange()).as("not yet past replicaLagMax").isEmpty();
            clock.advance(Duration.ofMillis(1));
            IsrProposal proposal = leader.proposeIsrChange().orElseThrow();

            assertThat(proposal.shard()).isEqualTo(SHARD);
            assertThat(proposal.expectedVersion()).isEqualTo(1);
            assertThat(proposal.state()).isEqualTo(new ShardState(LEADER, 1, List.of(LEADER),
                    List.of("broker-1", "broker-2")));
            assertThat(enqueued).as("the new set does not count until the coordinator has it").isNotDone();

            leader.isrChanged(new Versioned<>(proposal.state(), 2));

            assertThat(enqueued).isCompleted();
            assertThat(leader.status().isr()).containsExactly(LEADER);
            assertThat(leader.status().highWatermark()).isEqualTo(1);
        }

        @Test
        void aFollowerWithAnOpenStreamThatStopsConfirmingIsDropped() {
            ShardReplica leader = leader(log(), 1);
            leader.openFetch(new FetchRequest(SHARD, FOLLOWER, 1, 0, -1), new Recorder());
            CompletableFuture<MessageId> enqueued = leader.enqueue("a", "HIGH", null);

            clock.advance(LAG_MAX.plusMillis(1));
            IsrProposal proposal = leader.proposeIsrChange().orElseThrow();

            assertThat(proposal.state().isr()).containsExactly(LEADER);
            leader.isrChanged(new Versioned<>(proposal.state(), 2));
            assertThat(enqueued).isCompleted();
            assertThat(leader.proposeIsrChange()).as("it is not added back while it is behind").isEmpty();
        }

        @Test
        void onlyOneChangeIsInProgressAtATime() {
            ShardReplica leader = leader(log(), 1);
            leader.enqueue("a", "HIGH", null);
            clock.advance(LAG_MAX.plusMillis(1));

            assertThat(leader.proposeIsrChange()).isPresent();
            assertThat(leader.proposeIsrChange()).isEmpty();

            leader.isrChangeRefused();
            assertThat(leader.proposeIsrChange()).isPresent();
        }

        @Test
        void aFollowerThatCatchesUpIsAddedBack() {
            ShardReplica leader = leader(log(), 1);
            leader.enqueue("a", "HIGH", null);
            clock.advance(LAG_MAX.plusMillis(1));
            leader.isrChanged(new Versioned<>(leader.proposeIsrChange().orElseThrow().state(), 2));
            ShardReplica follower = follower(log(), 1);

            connect(follower, leader).start();
            IsrProposal proposal = leader.proposeIsrChange().orElseThrow();

            assertThat(proposal.expectedVersion()).isEqualTo(2);
            assertThat(proposal.state().isr()).containsExactly(LEADER, FOLLOWER);
            leader.isrChanged(new Versioned<>(proposal.state(), 3));
            assertThat(leader.status().isr()).containsExactly(LEADER, FOLLOWER);
            assertThat(follower.snapshot()).isEqualTo(leader.snapshot());
        }

        @Test
        void enqueuesAreRefusedBelowTheMinimumButConsumersCanDrain() {
            config = config.toBuilder().minInSyncReplicas(2).build();
            ShardReplica leader = leader(log(), 1);
            FetchStream stream = leader.openFetch(new FetchRequest(SHARD, FOLLOWER, 1, 0, -1), new Recorder());
            CompletableFuture<MessageId> first = leader.enqueue("a", "HIGH", null);
            stream.reportLogEnd(1);
            assertThat(first).isCompleted();
            stream.close();
            leader.enqueue("b", "HIGH", null);
            clock.advance(LAG_MAX.plusMillis(1));
            leader.isrChanged(new Versioned<>(leader.proposeIsrChange().orElseThrow().state(), 2));

            assertThatExceptionOfType(NotEnoughReplicasException.class)
                    .isThrownBy(() -> leader.enqueue("c", "HIGH", null));
            assertThat(leader.enqueue("d", "HIGH", null, Acks.LEADER)).isCompleted();
            assertThat(leader.dequeue().join()).isPresent();
            assertThat(leader.ack(first.join()).join()).isTrue();
        }

        @Test
        void anEnqueueThatOutlivedItsReplicasIsNotReportedAsSafe() {
            config = config.toBuilder().minInSyncReplicas(2).build();
            ShardReplica leader = leader(log(), 1);
            CompletableFuture<MessageId> enqueued = leader.enqueue("a", "HIGH", null);

            clock.advance(LAG_MAX.plusMillis(1));
            leader.isrChanged(new Versioned<>(leader.proposeIsrChange().orElseThrow().state(), 2));

            assertThat(enqueued).failsWithin(1, TimeUnit.SECONDS).withThrowableThat()
                    .withCauseInstanceOf(NotEnoughReplicasException.class);
        }
    }

    private ShardReplica leader(InMemoryShardLog log, int epoch) {
        ShardReplica replica = new ShardReplica(SHARD, LEADER, config, log, clock, LAG_MAX, Duration.ofSeconds(10));
        replica.lead(state(epoch));
        return replica;
    }

    private ShardReplica follower(InMemoryShardLog log, int epoch) {
        ShardReplica replica = new ShardReplica(SHARD, FOLLOWER, config, log, clock, LAG_MAX, Duration.ofSeconds(10));
        replica.follow(state(epoch));
        return replica;
    }

    private static Versioned<ShardState> state(int epoch) {
        return new Versioned<>(
                new ShardState(LEADER, epoch, List.of(LEADER, FOLLOWER), List.of("broker-1", "broker-2")), 1);
    }

    /** A fetcher that calls the leader directly, so everything happens on the calling thread. */
    private FollowerFetcher connect(ShardReplica follower, ShardReplica leader) {
        return new FollowerFetcher(follower, (request, toFollower) -> Optional.of(leader.openFetch(request, toFollower)),
                () -> { }, scheduler, Duration.ofMillis(10));
    }

    /** A log holding one enqueue per given epoch, in order. */
    private InMemoryShardLog log(int... epochs) {
        InMemoryShardLog log = new InMemoryShardLog();
        for (int epoch : epochs) {
            log.append(epoch, new ShardEvent.Enqueued("m" + log.endOffset() + "/" + epoch, 0, clock.instant(), null));
        }
        return log;
    }

    private static final class Recorder implements FetchListener {

        final List<LogEntry> entries = new ArrayList<>();
        final List<Long> highWatermarks = new ArrayList<>();
        final List<FetchEnd> ends = new ArrayList<>();

        @Override
        public void onEntries(List<LogEntry> batch, long highWatermark) {
            entries.addAll(batch);
            highWatermarks.add(highWatermark);
        }

        @Override
        public void onEnd(FetchEnd end) {
            ends.add(end);
        }
    }
}
