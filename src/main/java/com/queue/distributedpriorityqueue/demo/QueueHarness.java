package com.queue.distributedpriorityqueue.demo;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * Producers and consumers that drive a running broker over its REST API and then check that
 * nothing was lost. Some consumers deliberately skip the acknowledgement, which exercises
 * redelivery and dead-lettering.
 *
 * <p>Run it with {@code ./gradlew harness --args="--url=http://localhost:8080"}. Every option has
 * a default; see {@link Options}.
 */
public final class QueueHarness {

    private static final List<String> LEVELS = List.of("HIGH", "MEDIUM", "LOW");
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final Options options;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final String queue = "harness-" + System.currentTimeMillis();

    private final Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();
    private final AtomicLong refusedAcks = new AtomicLong();
    private final List<Long> enqueueNanos = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> dequeueNanos = Collections.synchronizedList(new ArrayList<>());
    private final List<Long> ackNanos = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean done = new AtomicBoolean();

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
            for (Future<?> producer : producers) {
                producer.get(options.timeout().toSeconds(), TimeUnit.SECONDS);
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
            String level = LEVELS.get(ThreadLocalRandom.current().nextInt(LEVELS.size()));
            String body = JSON.writeValueAsString(Map.of("payload", "message-" + message, "priority", level));
            long start = System.nanoTime();
            HttpResponse<String> response = post("/queues/" + queue + "/messages", body);
            enqueueNanos.add(System.nanoTime() - start);
            if (response.statusCode() != 201) {
                throw new IllegalStateException("enqueue failed: " + response.statusCode() + " " + response.body());
            }
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

    /** A message is settled once it is acknowledged, dead-lettered or expired. */
    private boolean waitUntilEveryMessageIsSettled(long started) throws Exception {
        while (System.nanoTime() - started < options.timeout().toNanos()) {
            JsonNode metrics = metrics();
            long settled = metrics.get("ackedTotal").asLong() + metrics.get("deadLetteredTotal").asLong()
                    + metrics.get("expiredTotal").asLong();
            if (settled >= options.messages()) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private Report report(boolean settled, Duration elapsed) throws Exception {
        JsonNode metrics = metrics();
        JsonNode deadLetterList = JSON.readTree(get("/queues/" + queue + "/dlq").body());
        long totalDeliveries = deliveries.values().stream().mapToLong(AtomicInteger::get).sum();
        return new Report(queue, options.messages(), settled, elapsed,
                metrics.get("enqueuedTotal").asLong(), deliveries.size(), totalDeliveries,
                metrics.get("ackedTotal").asLong(), metrics.get("deadLetteredTotal").asLong(),
                metrics.get("expiredTotal").asLong(), deadLetterList.size(), metrics.get("readyTotal").asLong(),
                metrics.get("inFlight").asLong(), refusedAcks.get(), Latency.of(enqueueNanos),
                Latency.of(dequeueNanos), Latency.of(ackNanos));
    }

    private JsonNode metrics() throws Exception {
        return JSON.readTree(get("/queues/" + queue + "/metrics").body());
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(options.url().resolve(path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(options.url().resolve(path)).timeout(Duration.ofSeconds(30)).build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /**
     * @param skipAckPercent how often a consumer takes a message and never acknowledges it
     * @param timeout how long to wait for every message to be settled before giving up
     */
    public record Options(URI url, int producers, int consumers, int messages, int skipAckPercent, int shards,
            Duration visibilityTimeout, int maxRetries, Duration timeout) {

        public static Options defaults(URI url) {
            return new Options(url, 4, 8, 2000, 20, 3, Duration.ofSeconds(1), 3, Duration.ofMinutes(2));
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
            Options defaults = defaults(URI.create(given.getOrDefault("url", "http://localhost:8080")));
            return new Options(defaults.url(),
                    number(given, "producers", defaults.producers()),
                    number(given, "consumers", defaults.consumers()),
                    number(given, "messages", defaults.messages()),
                    number(given, "skipAckPercent", defaults.skipAckPercent()),
                    number(given, "shards", defaults.shards()),
                    Duration.ofMillis(number(given, "visibilityTimeoutMs", (int) defaults.visibilityTimeout().toMillis())),
                    number(given, "maxRetries", defaults.maxRetries()),
                    Duration.ofSeconds(number(given, "timeoutSeconds", (int) defaults.timeout().toSeconds())));
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
     * What happened, as the broker's metrics and the consumers saw it.
     *
     * @param distinctDelivered how many different messages reached a consumer at least once
     * @param refusedAcks acks the broker answered with 404, because the message was already settled
     */
    public record Report(String queue, int produced, boolean settledInTime, Duration elapsed, long enqueued,
            int distinctDelivered, long deliveries, long acked, long deadLettered, long expired, int deadLetterList,
            long ready, long inFlight, long refusedAcks, Latency enqueueLatency, Latency dequeueLatency,
            Latency ackLatency) {

        /** Nothing lost: every message was enqueued, delivered at least once and settled, and nothing is left over. */
        public boolean passed() {
            return settledInTime
                    && enqueued == produced
                    && distinctDelivered == produced
                    && acked + deadLettered + expired == produced
                    && deadLetterList == deadLettered
                    && ready == 0
                    && inFlight == 0;
        }

        public String describe() {
            return String.join(System.lineSeparator(),
                    "Queue " + queue + " after " + elapsed.toMillis() + " ms",
                    "  produced            " + produced,
                    "  enqueued            " + enqueued,
                    "  delivered at least once  " + distinctDelivered,
                    "  deliveries          " + deliveries + " (" + (deliveries - distinctDelivered) + " redeliveries)",
                    "  acknowledged        " + acked,
                    "  dead-lettered       " + deadLettered + " (" + deadLetterList + " listed)",
                    "  expired             " + expired,
                    "  left ready / in flight   " + ready + " / " + inFlight,
                    "  acks refused as already settled   " + refusedAcks,
                    "  enqueue latency     " + enqueueLatency,
                    "  dequeue latency     " + dequeueLatency,
                    "  ack latency         " + ackLatency,
                    passed() ? "PASSED: no message was lost" : "FAILED: the numbers above do not add up");
        }
    }
}
