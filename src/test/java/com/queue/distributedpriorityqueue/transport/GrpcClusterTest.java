package com.queue.distributedpriorityqueue.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.queue.distributedpriorityqueue.cluster.Broker;
import com.queue.distributedpriorityqueue.cluster.QueueMetrics;
import com.queue.distributedpriorityqueue.cluster.QueueService;
import com.queue.distributedpriorityqueue.coordination.BrokerInfo;
import com.queue.distributedpriorityqueue.coordination.InMemoryCoordinator;
import com.queue.distributedpriorityqueue.coordination.ReplicaId;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.exception.BrokerUnreachableException;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.ShardReplica;
import com.queue.distributedpriorityqueue.testsupport.ManualClock;
import com.queue.distributedpriorityqueue.testsupport.TestBrokers;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Three brokers in one JVM that reach each other the way separate processes do: through real
 * gRPC servers on localhost ports. Replication, passing requests on and failover must behave as
 * they do over the in-process network the other cluster tests use.
 */
@Timeout(60)
class GrpcClusterTest {

    private static final ShardId SHARD = new ShardId("orders", 0);
    private static final Duration SOON = Duration.ofSeconds(10);
    private static final List<String> BROKERS = List.of("broker-1", "broker-2", "broker-3");

    private final ManualClock clock = new ManualClock();
    private final InMemoryCoordinator coordinator = new InMemoryCoordinator();
    private final Map<String, Node> nodes = new LinkedHashMap<>();

    private record Node(Broker broker, GrpcBrokerServer server, GrpcBrokerTransport transport) {
    }

    @BeforeEach
    void startBrokers() throws IOException {
        for (String id : BROKERS) {
            start(id);
        }
    }

    @AfterEach
    void stop() {
        for (Node node : nodes.values()) {
            node.server().close();
            node.broker().close();
            node.transport().close();
        }
        coordinator.close();
    }

    @Test
    void aReplicatedShardAgreesOnEveryBroker() {
        queues("broker-1").createQueue(QueueConfig.builder("orders").replicationFactor(3).minInSyncReplicas(2).build());
        List<MessageId> ids = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            ids.add(queues(BROKERS.get(i % 3)).enqueue("orders", "m" + i, i % 3 == 0 ? "HIGH" : "LOW", null, null)
                    .join());
        }
        for (int i = 0; i < 30; i++) {
            assertThat(queues(BROKERS.get(i % 3)).dequeue("orders", Duration.ZERO).join()).isPresent();
        }
        for (int i = 0; i < 10; i++) {
            assertThat(queues(BROKERS.get(i % 3)).ack("orders", ids.get(i * 3)).join()).isTrue();
        }

        String leader = leaderOf(SHARD);
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ShardReplica onLeader = replica(leader, SHARD);
            assertThat(onLeader.status().highWatermark()).isEqualTo(100);
            for (String broker : BROKERS) {
                assertThat(replica(broker, SHARD).status().highWatermark()).isEqualTo(100);
                assertThat(replica(broker, SHARD).snapshot()).isEqualTo(onLeader.snapshot());
            }
        });
        assertThat(replica(leader, SHARD).snapshot().liveMessages()).hasSize(50);
    }

    @Test
    void anyBrokerServesAnyRequestAndGathersFromEveryLeader() {
        queues("broker-1").createQueue(QueueConfig.builder("orders").shards(3).maxRetries(0).build());
        Set<String> leaders = new HashSet<>();
        for (int shard = 0; shard < 3; shard++) {
            leaders.add(leaderOf(new ShardId("orders", shard)));
        }
        assertThat(leaders).containsExactlyInAnyOrderElementsOf(BROKERS);
        for (int i = 0; i < 9; i++) {
            queues("broker-1").enqueue("orders", "low-" + i, "LOW", null, null).join();
        }
        MessageId high = queues("broker-1").enqueue("orders", "high", "HIGH", null, "a-key").join();
        String consumer = BROKERS.stream()
                .filter(broker -> !broker.equals(leaderOf(new ShardId("orders", high.shard())))).findFirst().orElseThrow();

        nodes.get(consumer).broker().exchangeSummaries().join();
        assertThat(dequeue(consumer)).map(MessageView::payload).contains("high");
        assertThat(queues("broker-3").ack("orders", high).join()).isTrue();
        List<String> lows = new ArrayList<>();
        for (Optional<MessageView> next = dequeue("broker-2"); next.isPresent(); next = dequeue("broker-2")) {
            lows.add(next.get().payload());
        }
        assertThat(lows).hasSize(9);

        clock.advance(Duration.ofSeconds(30));
        assertThat(dequeue("broker-3")).as("nine unacknowledged messages are dead-lettered").isEmpty();

        for (String broker : BROKERS) {
            QueueMetrics metrics = queues(broker).metrics("orders").join();
            assertThat(metrics.enqueuedTotal()).isEqualTo(10);
            assertThat(metrics.ackedTotal()).isEqualTo(1);
            assertThat(metrics.deadLetteredTotal()).isEqualTo(9);
            assertThat(queues(broker).deadLetters("orders").join()).hasSize(9);
        }
    }

    @Test
    void errorsArriveAsTheExceptionTheOtherBrokerThrew() throws IOException {
        queues("broker-1").createQueue(QueueConfig.builder("orders").replicationFactor(3).minInSyncReplicas(2).build());
        String leader = leaderOf(SHARD);
        String follower = BROKERS.stream().filter(broker -> !broker.equals(leader)).findFirst().orElseThrow();
        await().until(() -> nodes.get(follower).broker().replica(SHARD).isPresent());
        GrpcBrokerTransport transport = nodes.get(leader).transport();

        assertThat(transport.connect(nodes.get(follower).broker().info()).enqueue(SHARD, 0, "a", "HIGH", null, Acks.ALL))
                .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(NotLeaderException.class);
        assertThat(queues(follower).enqueue("orders", "a", "URGENT", null, null))
                .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(IllegalArgumentException.class)
                .withMessageContaining("unknown priority level");
        BrokerInfo nobody = new BrokerInfo("broker-9", "none", "localhost", 1, freePort());
        assertThat(transport.connect(nobody).dequeue(SHARD, 0))
                .failsWithin(SOON).withThrowableThat().withCauseInstanceOf(BrokerUnreachableException.class);
    }

    @Test
    void anInSyncReplicaTakesOverWhenTheLeaderDies() {
        queues("broker-1").createQueue(QueueConfig.builder("orders").replicationFactor(3).minInSyncReplicas(2).build());
        String oldLeader = leaderOf(SHARD);
        for (int i = 0; i < 10; i++) {
            queues(oldLeader).enqueue("orders", "m" + i, "HIGH", null, null).join();
        }
        await().until(() -> BROKERS.stream().allMatch(broker -> nodes.get(broker).broker().replica(SHARD).isPresent()
                && replica(broker, SHARD).status().highWatermark() == 10));

        crash(oldLeader);

        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            ReplicaId leader = coordinator.shard(SHARD).orElseThrow().value().leader();
            return !leader.brokerId().equals(oldLeader) && replica(leader.brokerId(), SHARD).isLeader();
        });
        String newLeader = leaderOf(SHARD);
        String follower = nodes.keySet().stream().filter(broker -> !broker.equals(newLeader)).findFirst().orElseThrow();
        assertThat(coordinator.shard(SHARD).orElseThrow().value().epoch()).isEqualTo(1);
        await().untilAsserted(() ->
                assertThat(queues(follower).enqueue("orders", "after", "LOW", null, null)).succeedsWithin(SOON));
        List<String> delivered = new ArrayList<>();
        for (Optional<MessageView> next = dequeue(follower); next.isPresent(); next = dequeue(follower)) {
            delivered.add(next.get().payload());
        }
        assertThat(delivered).contains("m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "after");
    }

    @Test
    void aRestartedFollowerCatchesUpOverGrpc() throws IOException {
        queues("broker-1").createQueue(QueueConfig.builder("orders").replicationFactor(3).minInSyncReplicas(2).build());
        String leader = leaderOf(SHARD);
        String follower = BROKERS.stream().filter(broker -> !broker.equals(leader)).findFirst().orElseThrow();
        for (int i = 0; i < 20; i++) {
            queues(leader).enqueue("orders", "m" + i, "HIGH", null, null).join();
        }

        crash(follower);
        ReplicaId restarted = start(follower).info().replicaId();

        await().atMost(10, TimeUnit.SECONDS).until(() -> nodes.get(follower).broker().replica(SHARD).isPresent()
                && replica(follower, SHARD).status().logEnd() == 20);
        nodes.get(leader).broker().maintainReplication();
        await().untilAsserted(() -> assertThat(replica(leader, SHARD).status().isr()).contains(restarted));
        await().untilAsserted(() ->
                assertThat(replica(follower, SHARD).snapshot()).isEqualTo(replica(leader, SHARD).snapshot()));
    }

    private Broker start(String id) throws IOException {
        int port = freePort();
        GrpcBrokerTransport transport = new GrpcBrokerTransport(Duration.ofSeconds(15));
        Broker broker = new Broker(
                TestBrokers.settings(id, 8080, port, Duration.ofHours(1), Duration.ofSeconds(10)), coordinator,
                transport, clock);
        GrpcBrokerServer server = new GrpcBrokerServer(broker.api(), port);
        broker.start();
        nodes.put(id, new Node(broker, server, transport));
        return broker;
    }

    /** The broker dies: its server stops, and its lease runs out. */
    private void crash(String id) {
        Node node = nodes.remove(id);
        node.server().close();
        node.broker().kill();
        node.transport().close();
        coordinator.expireLease(id);
    }

    private QueueService queues(String broker) {
        return nodes.get(broker).broker().queues();
    }

    private ShardReplica replica(String broker, ShardId shard) {
        return nodes.get(broker).broker().replica(shard).orElseThrow();
    }

    private String leaderOf(ShardId shard) {
        return coordinator.shard(shard).orElseThrow().value().leader().brokerId();
    }

    private Optional<MessageView> dequeue(String broker) {
        return queues(broker).dequeue("orders", Duration.ZERO).join();
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
