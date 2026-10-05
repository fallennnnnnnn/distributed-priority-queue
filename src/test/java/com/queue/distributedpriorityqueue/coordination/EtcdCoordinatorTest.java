package com.queue.distributedpriorityqueue.coordination;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** The coordinator contract against a real etcd, started in a container. Skipped when Docker is not running. */
@Testcontainers(disabledWithoutDocker = true)
class EtcdCoordinatorTest extends CoordinatorContract {

    @Container
    private static final GenericContainer<?> ETCD = new GenericContainer<>("quay.io/coreos/etcd:v3.5.17")
            .withExposedPorts(2379)
            .withCommand("etcd", "--listen-client-urls=http://0.0.0.0:2379",
                    "--advertise-client-urls=http://0.0.0.0:2379");

    private Client raw;
    private String endpoint;

    @BeforeEach
    void start() throws Exception {
        endpoint = "http://" + ETCD.getHost() + ":" + ETCD.getMappedPort(2379);
        raw = Client.builder().endpoints(endpoint).build();
        raw.getKVClient().delete(key("/dpq/"), DeleteOption.builder().isPrefix(true).build()).get();
        coordinator = new EtcdCoordinator(List.of(endpoint), Duration.ofSeconds(2));
    }

    @AfterEach
    void close() {
        coordinator.close();
        raw.close();
    }

    @Test
    void aBrokerThatStopsRenewingItsLeaseIsGoneAfterTheLeaseTime() {
        EtcdCoordinator dying = new EtcdCoordinator(List.of(endpoint), Duration.ofSeconds(2));
        dying.register(new BrokerInfo("broker-7", "first", "localhost", 8080, 9090), () -> { });
        assertThat(coordinator.liveBrokers()).extracting(BrokerInfo::id).containsExactly("broker-7");

        // Closing the connection stops the renewals without telling etcd anything, as a crash would.
        dying.close();

        assertThat(coordinator.liveBrokers()).as("still listed until the lease runs out").hasSize(1);
        await().atMost(10, TimeUnit.SECONDS).until(() -> coordinator.liveBrokers().isEmpty());
    }

    /** Revokes the lease the broker's key hangs on, which is what etcd does when a lease runs out. */
    @Override
    protected void expireLease(String brokerId) {
        try {
            long lease = raw.getKVClient().get(key("/dpq/brokers/" + brokerId), GetOption.DEFAULT).get()
                    .getKvs().get(0).getLease();
            raw.getLeaseClient().revoke(lease).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static ByteSequence key(String text) {
        return ByteSequence.from(text, UTF_8);
    }
}
