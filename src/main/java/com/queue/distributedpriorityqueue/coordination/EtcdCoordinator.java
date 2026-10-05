package com.queue.distributedpriorityqueue.coordination;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.queue.distributedpriorityqueue.core.QueueConfig;
import com.queue.distributedpriorityqueue.exception.CoordinatorUnavailableException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.watch.WatchEvent;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The coordinator kept in etcd.
 *
 * <pre>
 * /dpq/brokers/{id}               a live broker; tied to a lease, so it vanishes when the broker stops renewing
 * /dpq/queues/{name}/config       a queue's settings
 * /dpq/queues/{name}/shards/{n}   a shard's leader, epoch and in-sync replicas
 * </pre>
 *
 * <p>A value's version is the etcd revision that last changed its key. A compare-and-swap is a
 * transaction that writes only if that revision is still the one the caller read.
 */
public final class EtcdCoordinator implements Coordinator {

    private static final String ROOT = "/dpq/";
    private static final String BROKERS = ROOT + "brokers/";
    private static final String QUEUES = ROOT + "queues/";
    private static final String CONFIG = "/config";
    private static final String SHARDS = "/shards/";

    private final Client client;
    /** How long a broker's lease lasts without renewal: the time it takes the others to notice a dead broker. */
    private final Duration leaseTime;
    private final Duration requestTimeout;
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "etcd-lease-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    public EtcdCoordinator(List<String> endpoints, Duration leaseTime) {
        this.client = Client.builder().endpoints(endpoints.toArray(String[]::new)).build();
        this.leaseTime = leaseTime;
        this.requestTimeout = Duration.ofMillis(Math.max(1000, Math.min(leaseTime.toMillis(), 5000)));
    }

    @Override
    public Registration register(BrokerInfo broker, Runnable onLeaseLost) {
        long ttlSeconds = Math.max(1, leaseTime.toSeconds());
        long leaseId = await(client.getLeaseClient().grant(ttlSeconds)).getID();
        ByteSequence key = bytes(BROKERS + broker.id());
        PutOption withLease = PutOption.builder().withLeaseId(leaseId).build();
        TxnResponse registered = await(client.getKVClient().txn()
                .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0)))
                .Then(Op.put(key, bytes(MetadataJson.write(broker)), withLease))
                .commit());
        if (!registered.isSucceeded()) {
            client.getLeaseClient().revoke(leaseId);
            throw new IllegalStateException("a broker with ID " + broker.id() + " is already registered");
        }
        return new Lease(leaseId, ttlSeconds, onLeaseLost);
    }

    @Override
    public List<BrokerInfo> liveBrokers() {
        return get(BROKERS, true).stream()
                .map(entry -> MetadataJson.read(text(entry.getValue()), BrokerInfo.class))
                .sorted(Comparator.comparing(BrokerInfo::id))
                .toList();
    }

    @Override
    public boolean createQueue(QueueConfig config, List<ShardState> shardStates) {
        if (shardStates.size() != config.shards()) {
            throw new IllegalArgumentException(
                    "queue " + config.name() + " has " + config.shards() + " shards but " + shardStates.size() + " states");
        }
        ByteSequence configKey = bytes(configKey(config.name()));
        List<Op> writes = new ArrayList<>();
        writes.add(Op.put(configKey, bytes(MetadataJson.write(config)), PutOption.DEFAULT));
        for (int shard = 0; shard < shardStates.size(); shard++) {
            writes.add(Op.put(bytes(shardKey(new ShardId(config.name(), shard))),
                    bytes(MetadataJson.write(shardStates.get(shard))), PutOption.DEFAULT));
        }
        return await(client.getKVClient().txn()
                .If(new Cmp(configKey, Cmp.Op.EQUAL, CmpTarget.version(0)))
                .Then(writes.toArray(Op[]::new))
                .commit()).isSucceeded();
    }

    @Override
    public Optional<Versioned<QueueConfig>> queue(String name) {
        return read(configKey(name), QueueConfig.class);
    }

    @Override
    public List<String> queueNames() {
        return get(QUEUES, true).stream()
                .map(entry -> text(entry.getKey()))
                .filter(key -> key.endsWith(CONFIG))
                .map(key -> key.substring(QUEUES.length(), key.length() - CONFIG.length()))
                .sorted()
                .toList();
    }

    @Override
    public boolean updateQueue(String name, long expectedVersion, QueueConfig config) {
        return compareAndSwap(configKey(name), expectedVersion, config).isPresent();
    }

    @Override
    public Optional<Versioned<ShardState>> shard(ShardId id) {
        return read(shardKey(id), ShardState.class);
    }

    @Override
    public OptionalLong updateShard(ShardId id, long expectedVersion, ShardState state) {
        return compareAndSwap(shardKey(id), expectedVersion, state);
    }

    @Override
    public Subscription watch(Listener listener) {
        Watch.Watcher watcher = client.getWatchClient().watch(bytes(ROOT), WatchOption.builder().isPrefix(true).build(),
                Watch.listener(response -> {
                    for (WatchEvent event : response.getEvents()) {
                        report(text(event.getKeyValue().getKey()), listener);
                    }
                }));
        return watcher::close;
    }

    @Override
    public void close() {
        watchdog.shutdownNow();
        client.close();
    }

    private static void report(String key, Listener listener) {
        if (key.startsWith(BROKERS)) {
            listener.onBrokersChanged();
        } else if (key.startsWith(QUEUES)) {
            String rest = key.substring(QUEUES.length());
            if (rest.endsWith(CONFIG)) {
                listener.onQueueChanged(rest.substring(0, rest.length() - CONFIG.length()));
            } else {
                int shards = rest.lastIndexOf(SHARDS);
                if (shards > 0) {
                    listener.onShardChanged(new ShardId(rest.substring(0, shards),
                            Integer.parseInt(rest.substring(shards + SHARDS.length()))));
                }
            }
        }
    }

    private <T> Optional<Versioned<T>> read(String key, Class<T> type) {
        return get(key, false).stream().findFirst().map(entry ->
                new Versioned<>(MetadataJson.read(text(entry.getValue()), type), entry.getModRevision()));
    }

    private List<KeyValue> get(String key, boolean prefix) {
        GetResponse response = await(client.getKVClient().get(bytes(key), GetOption.builder().isPrefix(prefix).build()));
        return response.getKvs();
    }

    /** Writes the value only if the key was last changed at {@code expectedVersion}. Returns the new version. */
    private OptionalLong compareAndSwap(String key, long expectedVersion, Object value) {
        ByteSequence keyBytes = bytes(key);
        TxnResponse response = await(client.getKVClient().txn()
                .If(new Cmp(keyBytes, Cmp.Op.EQUAL, CmpTarget.modRevision(expectedVersion)))
                .Then(Op.put(keyBytes, bytes(MetadataJson.write(value)), PutOption.DEFAULT))
                .commit());
        return response.isSucceeded() ? OptionalLong.of(response.getHeader().getRevision()) : OptionalLong.empty();
    }

    private <T> T await(CompletableFuture<T> answer) {
        try {
            return answer.get(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CoordinatorUnavailableException("interrupted while waiting for etcd");
        } catch (ExecutionException | TimeoutException e) {
            throw new CoordinatorUnavailableException("etcd did not answer: " + e);
        }
    }

    private static String configKey(String queue) {
        return QUEUES + queue + CONFIG;
    }

    private static String shardKey(ShardId shard) {
        return QUEUES + shard.queue() + SHARDS + shard.shard();
    }

    private static ByteSequence bytes(String text) {
        return ByteSequence.from(text, UTF_8);
    }

    private static String text(ByteSequence bytes) {
        return bytes.toString(UTF_8);
    }

    /**
     * A broker's lease, kept alive in the background. The broker is told it has lost the lease
     * when etcd says so, and also when two thirds of the lease time pass without a renewal: by
     * then it must assume the worst, so that it stops acting as a leader before the others, who
     * see the lease run out a little later, elect a new one.
     */
    private final class Lease implements Registration {

        private final long id;
        private final Runnable onLost;
        private final AtomicBoolean over = new AtomicBoolean();
        private final CloseableClient keepAlive;
        private final ScheduledFuture<?> check;
        private volatile long lastRenewedNanos = System.nanoTime();

        Lease(long id, long ttlSeconds, Runnable onLost) {
            this.id = id;
            this.onLost = onLost;
            this.keepAlive = client.getLeaseClient().keepAlive(id, new StreamObserver<>() {
                @Override
                public void onNext(LeaseKeepAliveResponse renewal) {
                    lastRenewedNanos = System.nanoTime();
                }

                @Override
                public void onError(Throwable error) {
                    lost();
                }

                @Override
                public void onCompleted() {
                    lost();
                }
            });
            long patienceNanos = TimeUnit.SECONDS.toNanos(ttlSeconds) * 2 / 3;
            long checkMillis = Math.max(50, ttlSeconds * 1000 / 10);
            this.check = watchdog.scheduleWithFixedDelay(() -> {
                if (System.nanoTime() - lastRenewedNanos > patienceNanos) {
                    lost();
                }
            }, checkMillis, checkMillis, TimeUnit.MILLISECONDS);
        }

        private void lost() {
            if (over.compareAndSet(false, true)) {
                check.cancel(false);
                keepAlive.close();
                // If etcd can still be reached, make sure the others see this broker as gone.
                client.getLeaseClient().revoke(id);
                onLost.run();
            }
        }

        @Override
        public void close() {
            if (over.compareAndSet(false, true)) {
                check.cancel(false);
                keepAlive.close();
                try {
                    await(client.getLeaseClient().revoke(id));
                } catch (CoordinatorUnavailableException e) {
                    // The lease will run out by itself.
                }
            }
        }
    }
}
