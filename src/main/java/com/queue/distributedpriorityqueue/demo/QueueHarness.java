package com.queue.distributedpriorityqueue.demo;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Producers and consumers that drive running brokers over the REST API and then check that
 * nothing was lost. Some consumers can deliberately skip the acknowledgement, which exercises
 * redelivery and dead-lettering, and the harness can kill a shard's leader in mid-run, which
 * exercises failover.
 *
 * <p>Against one broker:
 * {@code ./gradlew harness --args="--urls=http://localhost:8080"}
 *
 * <p>Against the docker-compose cluster, killing a leader half way:
 * {@code ./gradlew harness --args="--urls=http://localhost:8081,http://localhost:8082,http://localhost:8083
 * --replicationFactor=3 --minInSyncReplicas=2 --skipAckPercent=0 --killCommand='docker compose kill {leader}'"}
 *
 * <p>Every option has a default; see {@link Options}.
 */
public final class QueueHarness {

    private static final List<String> LEVELS = List.of("HIGH", "MEDIUM", "LOW");
    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** The cluster cannot serve the request right now, for example while a shard has no leader. */
    private static final int UNAVAILABLE = 503;

    private final Options options;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final String queue = "harness-" + System.currentTimeMillis();
    private final AtomicInteger nextBroker = new AtomicInteger();

    /** Payloads whose enqueue a broker confirmed. None of these may go missing. */
    private final Set<String> acknowledged = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong refusedAcks = new AtomicLong();
    private final List<Long> enqueueNanos = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> dequeueNanos = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> ackNanos = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean done = new AtomicBoolean();
    private volatile String killedBroker;
    private volatile Duration failoverTime;

    private QueueHarness(Options options) {
        this.options = options;
    }

    public static void main(String[] args) throws Exception {
        Report report = run(Options.parse(args));
        System.out.println(report.describe());
        System.exit(report.passed() ? 0 : 1);
    }

    public static Report run(Options options) throws Exception {
        return new QueueHarness(options).run();
    }

    private Report run() throws Exception {
        createQueue();
        long started = System.nanoTime();
        ExecutorService threads = Executors.newCachedThreadPool();
        try {
            List<Future<?>> producers = new ArrayList<>();
            AtomicInteger nextMessage = new AtomicInteger();
            for (int producer = 0; producer < options.producers(); producer++) {
                producers.add(threads.submit(() -> {
                    produce(nextMessage);
                    return null;
                }));
            }
            List<Future<?>> consumers = new ArrayList<>();
            for (int consumer = 0; consumer < options.consumers(); consumer++) {
                consumers.add(threads.submit(() -> {
                    consume();
                    return null;
                }));
            }
            Future<?> killer = options.killCommand() == null ? null : threads.submit(() -> {
                killALeaderHalfWay();
                return null;
            });
            for (Future<?> producer : producers) {
                producer.get(options.timeout().toSeconds(), TimeUnit.SECONDS);
            }
            if (killer != null) {
                killer.get(options.timeout().toSeconds(), TimeUnit.SECONDS);
            }
            boolean settled = waitUntilEveryMessageIsSettled(started);
            done.set(true);
            for (Future<?> consumer : consumers) {
                consumer.get(30, TimeUnit.SECONDS);
            }
            return report(settled, Duration.ofNanos(System.nanoTime() - started));
        } finally {
            done.set(true);
            threads.shutdownNow();
        }
    }

    private void createQueue() throws Exception {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("name", queue);
        request.put("shards", options.shards());
        request.put("replicationFactor", options.replicationFactor());
        request.put("minInSyncReplicas", options.minInSyncReplicas());
        request.put("visibilityTimeoutMs", options.visibilityTimeout().toMillis());
        request.put("maxRetries", options.maxRetries());
        HttpResponse<String> response = post("/queues", JSON.writeValueAsString(request));
        if (response.statusCode() != 201) {
            throw new IllegalStateException("could not create the queue: " + response.statusCode() + " " + response.body());
        }
    }

    private void produce(AtomicInteger nextMessage) throws Exception {
        for (int message = nextMessage.getAndIncrement(); message < options.messages();
                message = nextMessage.getAndIncrement()) {
            String payload = "message-" + message;
            String level = LEVELS.get(ThreadLocalRandom.current().nextInt(LEVELS.size()));
            String body = JSON.writeValueAsString(Map.of("payload", payload, "priority", level));
            long start = System.nanoTime();
            HttpResponse<String> response = post("/queues/" + queue + "/messages", body);
            enqueueNanos.add(System.nanoTime() - start);
            if (response.statusCode() != 201) {
                throw new IllegalStateException("enqueue failed: " + response.statusCode() + " " + response.body());
            }
            acknowledged.add(payload);
        }
    }

    private void consume() throws Exception {
        while (!done.get()) {
            long start = System.nanoTime();
            HttpResponse<String> response = post("/queues/" + queue + "/messages/dequeue", "");
            if (response.statusCode() == 204) {
                Thread.sleep(2);
                continue;
            }
            dequeueNanos.add(System.nanoTime() - start);
            if (response.statusCode() != 200) {
                throw new IllegalStateException("dequeue failed: " + response.statusCode() + " " + response.body());
            }
            JsonNode message = JSON.readTree(response.body());
            deliveries.computeIfAbsent(message.get("payload").asString(), payload -> new AtomicInteger())
                    .incrementAndGet();
            if (ThreadLocalRandom.current().nextInt(100) < options.skipAckPercent()) {
                continue;
            }
            start = System.nanoTime();
            int status = post("/queues/" + queue + "/messages/" + message.get("messageId").asString() + "/ack", "")
                    .statusCode();
            ackNanos.add(System.nanoTime() - start);
            if (status == 404) {
                refusedAcks.incrementAndGet();
            } else if (status != 204) {
                throw new IllegalStateException("ack failed: " + status);
            }
        }
    }

    /**
     * Once half the messages are in, kills the broker that leads shard 0 and measures how long
     * the shard has no leader: from the kill until the brokers name a different one.
     */
    private void killALeaderHalfWay() throws Exception {
        while (acknowledged.size() < options.messages() / 2) {
            Thread.sleep(5);
        }
        String leader = leaderOfShardZero();
        long killedAt = System.nanoTime();
        Process kill = new ProcessBuilder("sh", "-c", options.killCommand().replace("{leader}", leader))
                .redirectErrorStream(true).start();
        String output = new String(kill.getInputStream().readAllBytes());
        if (kill.waitFor() != 0) {
            throw new IllegalStateException("the kill command failed: " + output);
        }
        killedBroker = leader;
        while (true) {
            String now = leaderOfShardZero();
            if (now != null && !now.equals(leader)) {
                failoverTime = Duration.ofNanos(System.nanoTime() - killedAt);
                return;
            }
            Thread.sleep(20);
        }
    }

    private String leaderOfShardZero() throws Exception {
        JsonNode leader = JSON.readTree(get("/queues/" + queue).body()).get("shardStates").get(0).get("leader");
        return leader.isNull() ? null : leader.asString();
    }

    /** A message is settled once it is acknowledged, dead-lettered or expired. */
    private boolean waitUntilEveryMessageIsSettled(long started) throws Exception {
        while (System.nanoTime() - started < options.timeout().toNanos()) {
            JsonNode metrics = metrics();
            long settled = metrics.get("ackedTotal").asLong() + metrics.get("deadLetteredTotal").asLong()
                    + metrics.get("expiredTotal").asLong();
            if (settled >= metrics.get("enqueuedTotal").asLong() && metrics.get("readyTotal").asLong() == 0
                    && metrics.get("inFlight").asLong() == 0) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private Report report(boolean settled, Duration elapsed) throws Exception {
        JsonNode metrics = metrics();
        JsonNode deadLetterList = JSON.readTree(get("/queues/" + queue + "/dlq").body());
        Set<String> lost = new HashSet<>(acknowledged);
        lost.removeAll(deliveries.keySet());
        long totalDeliveries = deliveries.values().stream().mapToLong(AtomicInteger::get).sum();
        long deliveredMoreThanOnce = deliveries.values().stream().filter(count -> count.get() > 1).count();
        return new Report(queue, options.messages(), settled, elapsed, acknowledged.size(), lost.size(),
                metrics.get("enqueuedTotal").asLong(), deliveries.size(), totalDeliveries, deliveredMoreThanOnce,
                metrics.get("ackedTotal").asLong(), metrics.get("deadLetteredTotal").asLong(),
                metrics.get("expiredTotal").asLong(), deadLetterList.size(), metrics.get("readyTotal").asLong(),
                metrics.get("inFlight").asLong(), refusedAcks.get(), retries.get(), killedBroker, failoverTime,
                Latency.of(enqueueNanos), Latency.of(dequeueNanos), Latency.of(ackNanos));
    }

    private JsonNode metrics() throws Exception {
        return JSON.readTree(get("/queues/" + queue + "/metrics").body());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return send(path, HttpRequest.newBuilder().header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)));
    }

    private HttpResponse<String> get(String path) throws Exception {
        return send(path, HttpRequest.newBuilder().GET());
    }

    /**
     * Sends to the brokers in turn. A broker that cannot be reached, or that answers that the
     * cluster cannot serve the request right now, means try the next one, the way a client behind
     * a load balancer would. Retrying an enqueue can leave a duplicate; that is at-least-once.
     */
    private HttpResponse<String> send(String path, HttpRequest.Builder request) throws Exception {
        long giveUpAt = System.nanoTime() + options.timeout().toNanos();
        while (true) {
            URI broker = options.urls().get(Math.floorMod(nextBroker.getAndIncrement(), options.urls().size()));
            try {
                HttpResponse<String> response = http.send(
                        request.copy().uri(broker.resolve(path)).timeout(Duration.ofSeconds(30)).build(),
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != UNAVAILABLE) {
                    return response;
                }
            } catch (IOException brokerIsDown) {
                // Fall through to the next broker.
            }
            if (System.nanoTime() > giveUpAt) {
                throw new IllegalStateException("no broker could serve " + path + " within " + options.timeout());
            }
            retries.incrementAndGet();
            Thread.sleep(20);
        }
    }

    /**
     * @param urls the brokers' REST addresses; requests go to them in turn
     * @param skipAckPercent how often a consumer takes a message and never acknowledges it
     * @param timeout how long to wait for every message to be settled before giving up
     * @param killCommand a shell command run half way through, with {@code {leader}} replaced by the
     *        ID of the broker that leads shard 0; null to kill nothing
     */
    public record Options(List<URI> urls, int producers, int consumers, int messages, int skipAckPercent, int shards,
            int replicationFactor, int minInSyncReplicas, Duration visibilityTimeout, int maxRetries, Duration timeout,
            String killCommand) {

        public static Options defaults(List<URI> urls) {
            return new Options(urls, 4, 8, 2000, 20, 3, 1, 1, Duration.ofSeconds(1), 3, Duration.ofMinutes(2), null);
        }

        /** Reads {@code --name=value} arguments; names match the record's components. */
        static Options parse(String[] args) {
            Map<String, String> given = new LinkedHashMap<>();
            for (String arg : args) {
                int equals = arg.indexOf('=');
                if (!arg.startsWith("--") || equals < 0) {
                    throw new IllegalArgumentException("expected --name=value, got " + arg);
                }
                given.put(arg.substring(2, equals), arg.substring(equals + 1));
            }
            List<URI> urls = Arrays.stream(given.getOrDefault("urls", "http://localhost:8080").split(","))
                    .map(String::trim).map(URI::create).toList();
            Options defaults = defaults(urls);
            return new Options(urls,
                    number(given, "producers", defaults.producers()),
                    number(given, "consumers", defaults.consumers()),
                    number(given, "messages", defaults.messages()),
                    number(given, "skipAckPercent", defaults.skipAckPercent()),
                    number(given, "shards", defaults.shards()),
                    number(given, "replicationFactor", defaults.replicationFactor()),
                    number(given, "minInSyncReplicas", defaults.minInSyncReplicas()),
                    Duration.ofMillis(number(given, "visibilityTimeoutMs", (int) defaults.visibilityTimeout().toMillis())),
                    number(given, "maxRetries", defaults.maxRetries()),
                    Duration.ofSeconds(number(given, "timeoutSeconds", (int) defaults.timeout().toSeconds())),
                    given.get("killCommand"));
        }

        private static int number(Map<String, String> given, String name, int fallback) {
            return given.containsKey(name) ? Integer.parseInt(given.get(name)) : fallback;
        }
    }

    public record Latency(int samples, double p50Millis, double p95Millis, double p99Millis) {

        static Latency of(List<Long> nanos) {
            List<Long> sorted;
            synchronized (nanos) {
                sorted = new ArrayList<>(nanos);
            }
            Collections.sort(sorted);
            return new Latency(sorted.size(), percentile(sorted, 50), percentile(sorted, 95), percentile(sorted, 99));
        }

        private static double percentile(List<Long> sorted, int percent) {
            if (sorted.isEmpty()) {
                return 0;
            }
            int index = (int) Math.ceil(percent / 100.0 * sorted.size()) - 1;
            return sorted.get(Math.max(index, 0)) / 1_000_000.0;
        }

        @Override
        public String toString() {
            return String.format("p50 %.1f ms, p95 %.1f ms, p99 %.1f ms (%d calls)", p50Millis, p95Millis, p99Millis,
                    samples);
        }
    }

    /**
     * What happened, as the brokers' metrics and the consumers saw it.
     *
     * @param acknowledged enqueues a broker confirmed
     * @param lost confirmed enqueues that no consumer ever received; must be 0
     * @param enqueued what the brokers counted; more than {@code produced} when a retried enqueue left a duplicate
     * @param distinctDelivered how many different messages reached a consumer at least once
     * @param deliveredMoreThanOnce messages a consumer received more than once
     * @param refusedAcks acks the broker answered with 404, because the message was already settled
     * @param retries requests sent again because a broker was down or the shard had no leader
     * @param killedBroker the broker the kill command was run for, or null
     * @param failoverTime how long shard 0 had no live leader, or null if nothing was killed
     */
    public record Report(String queue, int produced, boolean settledInTime, Duration elapsed, int acknowledged,
            int lost, long enqueued, int distinctDelivered, long deliveries, long deliveredMoreThanOnce, long acked,
            long deadLettered, long expired, int deadLetterList, long ready, long inFlight, long refusedAcks,
            long retries, String killedBroker, Duration failoverTime, Latency enqueueLatency, Latency dequeueLatency,
            Latency ackLatency) {

        /** Nothing lost: every message was confirmed, delivered at least once and settled, and nothing is left over. */
        public boolean passed() {
            return settledInTime
                    && acknowledged == produced
                    && lost == 0
                    && enqueued >= produced
                    && acked + deadLettered + expired == enqueued
                    && deadLetterList == deadLettered
                    && ready == 0
                    && inFlight == 0;
        }

        public String describe() {
            List<String> lines = new ArrayList<>(List.of(
                    "Queue " + queue + " after " + elapsed.toMillis() + " ms",
                    "  produced                  " + produced,
                    "  enqueues confirmed        " + acknowledged,
                    "  confirmed but never delivered   " + lost,
                    "  enqueued, as the brokers count  " + enqueued,
                    "  delivered at least once   " + distinctDelivered,
                    "  deliveries                " + deliveries + " (" + deliveredMoreThanOnce
                            + " messages more than once)",
                    "  acknowledged              " + acked,
                    "  dead-lettered             " + deadLettered + " (" + deadLetterList + " listed)",
                    "  expired                   " + expired,
                    "  left ready / in flight    " + ready + " / " + inFlight,
                    "  acks refused as already settled   " + refusedAcks,
                    "  requests retried          " + retries));
            if (killedBroker != null) {
                lines.add("  killed                    " + killedBroker + ", the leader of shard 0");
                lines.add("  shard 0 without a leader  " + failoverTime.toMillis() + " ms");
            }
            lines.add("  enqueue latency           " + enqueueLatency);
            lines.add("  dequeue latency           " + dequeueLatency);
            lines.add("  ack latency               " + ackLatency);
            lines.add(passed() ? "PASSED: no message was lost" : "FAILED: the numbers above do not add up");
            return String.join(System.lineSeparator(), lines);
        }
    }
}
