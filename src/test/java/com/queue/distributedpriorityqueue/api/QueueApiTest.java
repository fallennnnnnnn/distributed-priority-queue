package com.queue.distributedpriorityqueue.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.queue.distributedpriorityqueue.testsupport.ManualClock;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Every endpoint and error code, over real HTTP against a broker on a random port. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "dpq.advertised.http-port=8080")
@Import(QueueApiTest.ManualClockConfiguration.class)
class QueueApiTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final AtomicInteger QUEUE_NUMBER = new AtomicInteger();

    private final HttpClient http = HttpClient.newHttpClient();

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private ManualClock clock;

    /** Each test gets a queue of its own, because the broker is shared by the whole class. */
    private String queue;

    @BeforeEach
    void pickAQueueName() {
        queue = "queue-" + QUEUE_NUMBER.incrementAndGet();
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ManualClockConfiguration {

        @Bean
        @Primary
        ManualClock manualClock() {
            return new ManualClock();
        }
    }

    @Nested
    class CreateQueue {

        @Test
        void createsAQueueWithDefaults() throws Exception {
            HttpResponse<String> response = post("/queues", "{\"name\":\"" + queue + "\"}");

            assertThat(response.statusCode()).isEqualTo(201);
            assertThat(response.headers().firstValue("Location")).contains("/queues/" + queue);
            JsonNode body = json(response);
            assertThat(body.get("name").asString()).isEqualTo(queue);
            assertThat(body.get("priorityLevels")).extracting(JsonNode::asString).containsExactly("HIGH", "MEDIUM", "LOW");
            assertThat(body.get("visibilityTimeoutMs").asLong()).isEqualTo(30_000);
            assertThat(body.get("maxRetries").asInt()).isEqualTo(3);
            assertThat(body.get("defaultTtlMs").isNull()).isTrue();
            assertThat(body.get("shards").asInt()).isEqualTo(1);
            assertThat(body.get("replicationFactor").asInt()).isEqualTo(1);
            assertThat(body.get("minInSyncReplicas").asInt()).isEqualTo(1);
            assertThat(body.get("uncleanLeaderElection").asBoolean()).isFalse();
            assertThat(body.get("shardStates")).hasSize(1);
            JsonNode shard = body.get("shardStates").get(0);
            assertThat(shard.get("shard").asInt()).isZero();
            assertThat(shard.get("leader").asString()).isEqualTo("broker-1");
            assertThat(shard.get("epoch").asInt()).isZero();
            assertThat(shard.get("replicas")).extracting(JsonNode::asString).containsExactly("broker-1");
            assertThat(shard.get("isr")).extracting(JsonNode::asString).containsExactly("broker-1");
        }

        @Test
        void createsAQueueWithItsOwnSettings() throws Exception {
            HttpResponse<String> response = post("/queues", "{\"name\":\"" + queue + "\","
                    + "\"priorityLevels\":[\"urgent\",\"normal\"],\"visibilityTimeoutMs\":5000,\"maxRetries\":1,"
                    + "\"defaultTtlMs\":60000,\"shards\":3,\"uncleanLeaderElection\":true}");

            assertThat(response.statusCode()).isEqualTo(201);
            JsonNode body = json(response);
            assertThat(body.get("priorityLevels")).extracting(JsonNode::asString).containsExactly("urgent", "normal");
            assertThat(body.get("visibilityTimeoutMs").asLong()).isEqualTo(5000);
            assertThat(body.get("maxRetries").asInt()).isEqualTo(1);
            assertThat(body.get("defaultTtlMs").asLong()).isEqualTo(60_000);
            assertThat(body.get("shards").asInt()).isEqualTo(3);
            assertThat(body.get("uncleanLeaderElection").asBoolean()).isTrue();
            assertThat(body.get("shardStates")).hasSize(3);
        }

        @Test
        void aTakenNameIsAConflict() throws Exception {
            createQueue("{}");

            HttpResponse<String> response = post("/queues", "{\"name\":\"" + queue + "\"}");

            assertProblem(response, 409, "already exists");
        }

        @Test
        void invalidSettingsAreBadRequests() throws Exception {
            assertProblem(post("/queues", "{}"), 400, "queue name");
            assertProblem(post("/queues", "{\"name\":\"has space\"}"), 400, "queue name");
            assertProblem(post("/queues", "{\"name\":\"" + queue + "\",\"shards\":0}"), 400, "shards");
            assertProblem(post("/queues", "{\"name\":\"" + queue + "\",\"priorityLevels\":[]}"), 400, "priority levels");
            assertProblem(post("/queues", "{\"name\":\"" + queue + "\",\"visibilityTimeoutMs\":0}"), 400,
                    "visibilityTimeout");
            assertProblem(post("/queues", "{\"name\":\"" + queue + "\",\"replicationFactor\":2}"), 400, "live brokers");
            assertThat(post("/queues", "{not json").statusCode()).isEqualTo(400);
            assertThat(get("/queues/" + queue).statusCode()).isEqualTo(404);
        }
    }

    @Nested
    class DescribeAndUpdate {

        @Test
        void describesAQueue() throws Exception {
            createQueue("{\"shards\":2}");

            HttpResponse<String> response = get("/queues/" + queue);

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(json(response).get("name").asString()).isEqualTo(queue);
            assertThat(json(response).get("shardStates")).hasSize(2);
        }

        @Test
        void anUnknownQueueIsNotFound() throws Exception {
            assertProblem(get("/queues/" + queue), 404, "no queue named");
            assertProblem(patch("/queues/" + queue, "{\"maxRetries\":1}"), 404, "no queue named");
        }

        @Test
        void changesOnlyTheSettingsGiven() throws Exception {
            createQueue("{\"defaultTtlMs\":60000}");

            HttpResponse<String> response = patch("/queues/" + queue,
                    "{\"visibilityTimeoutMs\":1000,\"maxRetries\":9,\"uncleanLeaderElection\":true}");

            assertThat(response.statusCode()).isEqualTo(200);
            JsonNode body = json(get("/queues/" + queue));
            assertThat(body.get("visibilityTimeoutMs").asLong()).isEqualTo(1000);
            assertThat(body.get("maxRetries").asInt()).isEqualTo(9);
            assertThat(body.get("uncleanLeaderElection").asBoolean()).isTrue();
            assertThat(body.get("defaultTtlMs").asLong()).isEqualTo(60_000);
        }

        @Test
        void aDefaultTtlOfZeroRemovesIt() throws Exception {
            createQueue("{\"defaultTtlMs\":60000}");

            patch("/queues/" + queue, "{\"defaultTtlMs\":0}");

            assertThat(json(get("/queues/" + queue)).get("defaultTtlMs").isNull()).isTrue();
        }

        @Test
        void settingsFixedAtCreationCannotChange() throws Exception {
            createQueue("{}");

            assertProblem(patch("/queues/" + queue, "{\"shards\":2}"), 400, "fixed");
            assertProblem(patch("/queues/" + queue, "{\"replicationFactor\":1,\"priorityLevels\":[\"a\"]}"), 400, "fixed");
            assertProblem(patch("/queues/" + queue, "{\"maxRetries\":-1}"), 400, "maxRetries");
            assertThat(json(get("/queues/" + queue)).get("shards").asInt()).isEqualTo(1);
        }
    }

    @Nested
    class Enqueue {

        @Test
        void returnsAMessageId() throws Exception {
            createQueue("{}");

            HttpResponse<String> response = enqueue("{\"payload\":\"hello\",\"priority\":\"HIGH\"}");

            assertThat(response.statusCode()).isEqualTo(201);
            String messageId = json(response).get("messageId").asString();
            assertThat(messageId).isEqualTo("0-0");
            assertThat(response.headers().firstValue("Location")).contains("/queues/" + queue + "/messages/0-0");
        }

        @Test
        void badMessagesAreBadRequests() throws Exception {
            createQueue("{}");

            assertProblem(enqueue("{\"priority\":\"HIGH\"}"), 400, "payload");
            assertProblem(enqueue("{\"payload\":\"a\",\"priority\":\"URGENT\"}"), 400, "unknown priority level");
            assertProblem(enqueue("{\"payload\":\"a\",\"ttlMs\":0}"), 400, "ttl");
            assertProblem(enqueue("{\"payload\":\"a\",\"ttlMs\":1000,\"expiresAt\":\"2030-01-01T00:00:00Z\"}"), 400,
                    "not both");
            assertProblem(enqueue("{\"payload\":\"a\",\"expiresAt\":\"2020-01-01T00:00:00Z\"}"), 400, "future");
            assertThat(json(get("/queues/" + queue + "/metrics")).get("enqueuedTotal").asLong()).isZero();
        }

        @Test
        void anUnknownQueueIsNotFound() throws Exception {
            assertProblem(enqueue("{\"payload\":\"a\"}"), 404, "no queue named");
        }

        @Test
        void aMessageIsNotDeliveredAfterItsTtl() throws Exception {
            createQueue("{}");
            enqueue("{\"payload\":\"short\",\"ttlMs\":10000}");
            enqueue("{\"payload\":\"dated\",\"expiresAt\":\"" + clock.instant().plusSeconds(20) + "\"}");
            enqueue("{\"payload\":\"forever\"}");

            clock.advance(Duration.ofSeconds(10));
            assertThat(json(dequeue()).get("payload").asString()).isEqualTo("dated");
            clock.advance(Duration.ofSeconds(10));

            assertThat(json(dequeue()).get("payload").asString()).isEqualTo("forever");
            assertThat(dequeue().statusCode()).isEqualTo(204);
            assertThat(json(get("/queues/" + queue + "/metrics")).get("expiredTotal").asLong()).isEqualTo(1);
        }

        @Test
        void messagesWithTheSameShardKeyShareAShard() throws Exception {
            createQueue("{\"shards\":4}");

            String first = json(enqueue("{\"payload\":\"a\",\"shardKey\":\"customer-7\"}")).get("messageId").asString();
            String second = json(enqueue("{\"payload\":\"b\",\"shardKey\":\"customer-7\"}")).get("messageId").asString();

            assertThat(first.substring(0, first.indexOf('-'))).isEqualTo(second.substring(0, second.indexOf('-')));
        }
    }

    @Nested
    class Dequeue {

        @Test
        void deliversTheHighestPriorityMessage() throws Exception {
            createQueue("{}");
            enqueue("{\"payload\":\"later\",\"priority\":\"LOW\"}");
            enqueue("{\"payload\":\"first\",\"priority\":\"high\"}");

            HttpResponse<String> response = dequeue();

            assertThat(response.statusCode()).isEqualTo(200);
            JsonNode body = json(response);
            assertThat(body.get("messageId").asString()).isEqualTo("0-1");
            assertThat(body.get("payload").asString()).isEqualTo("first");
            assertThat(body.get("priority").asString()).isEqualTo("HIGH");
            assertThat(body.get("deliveryCount").asInt()).isEqualTo(1);
            assertThat(body.get("enqueuedAt").asString()).isEqualTo(clock.instant().toString());
            assertThat(body.get("visibleUntil").asString()).isEqualTo(clock.instant().plusSeconds(30).toString());
        }

        @Test
        void anEmptyQueueIsNoContent() throws Exception {
            createQueue("{}");

            HttpResponse<String> response = dequeue();

            assertThat(response.statusCode()).isEqualTo(204);
            assertThat(response.body()).isEmpty();
        }

        @Test
        void anUnknownQueueIsNotFound() throws Exception {
            assertProblem(dequeue(), 404, "no queue named");
        }

        @Test
        void theWaitIsLimited() throws Exception {
            createQueue("{}");

            assertProblem(post("/queues/" + queue + "/messages/dequeue?waitTimeMs=20001", ""), 400, "waitTime");
            assertProblem(post("/queues/" + queue + "/messages/dequeue?waitTimeMs=-1", ""), 400, "waitTime");
        }

        @Test
        void aWaitingDequeueReturnsWhenAMessageArrives() throws Exception {
            createQueue("{}");

            CompletableFuture<HttpResponse<String>> waiting = http.sendAsync(
                    request("/queues/" + queue + "/messages/dequeue?waitTimeMs=10000")
                            .POST(HttpRequest.BodyPublishers.noBody()).build(),
                    HttpResponse.BodyHandlers.ofString());
            Thread.sleep(200);
            assertThat(waiting).isNotDone();
            enqueue("{\"payload\":\"late\"}");

            HttpResponse<String> response = waiting.get(5, TimeUnit.SECONDS);
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(json(response).get("payload").asString()).isEqualTo("late");
        }

        @Test
        void anUnackedMessageIsRedeliveredAfterItsVisibilityTimeout() throws Exception {
            createQueue("{\"visibilityTimeoutMs\":5000}");
            enqueue("{\"payload\":\"a\"}");
            dequeue();
            assertThat(dequeue().statusCode()).isEqualTo(204);

            clock.advance(Duration.ofSeconds(5));

            JsonNode redelivery = json(dequeue());
            assertThat(redelivery.get("payload").asString()).isEqualTo("a");
            assertThat(redelivery.get("deliveryCount").asInt()).isEqualTo(2);
        }
    }

    @Nested
    class Acknowledge {

        @Test
        void removesTheMessage() throws Exception {
            createQueue("{}");
            enqueue("{\"payload\":\"a\"}");
            String messageId = json(dequeue()).get("messageId").asString();

            HttpResponse<String> response = ack(messageId);

            assertThat(response.statusCode()).isEqualTo(204);
            clock.advance(Duration.ofHours(1));
            assertThat(dequeue().statusCode()).isEqualTo(204);
            assertThat(json(get("/queues/" + queue + "/metrics")).get("ackedTotal").asLong()).isEqualTo(1);
        }

        @Test
        void nothingToAcknowledgeIsNotFound() throws Exception {
            createQueue("{}");
            String messageId = json(enqueue("{\"payload\":\"a\"}")).get("messageId").asString();

            assertProblem(ack(messageId), 404, "no message");
            dequeue();
            assertThat(ack(messageId).statusCode()).isEqualTo(204);
            assertProblem(ack(messageId), 404, "no message");
            assertProblem(ack("0-99"), 404, "no message");
            assertProblem(ack("7-0"), 404, "no message");
        }

        @Test
        void aMalformedIdIsABadRequest() throws Exception {
            createQueue("{}");

            assertProblem(ack("not-an-id"), 400, "not a message ID");
        }

        @Test
        void anUnknownQueueIsNotFound() throws Exception {
            assertProblem(ack("0-0"), 404, "no queue named");
        }
    }

    @Nested
    class MetricsAndDeadLetters {

        @Test
        void reportsQueueMetrics() throws Exception {
            createQueue("{\"shards\":2}");
            enqueue("{\"payload\":\"h\",\"priority\":\"HIGH\"}");
            enqueue("{\"payload\":\"l1\",\"priority\":\"LOW\"}");
            enqueue("{\"payload\":\"l2\",\"priority\":\"LOW\"}");
            clock.advance(Duration.ofSeconds(7));
            dequeue();

            HttpResponse<String> response = get("/queues/" + queue + "/metrics");

            assertThat(response.statusCode()).isEqualTo(200);
            JsonNode body = json(response);
            assertThat(body.get("queue").asString()).isEqualTo(queue);
            assertThat(body.get("ready").get("HIGH").asLong()).isZero();
            assertThat(body.get("ready").get("MEDIUM").asLong()).isZero();
            assertThat(body.get("ready").get("LOW").asLong()).isEqualTo(2);
            assertThat(body.get("readyTotal").asLong()).isEqualTo(2);
            assertThat(body.get("inFlight").asLong()).isEqualTo(1);
            assertThat(body.get("oldestMessageAgeSeconds").asDouble()).isEqualTo(7.0);
            assertThat(body.get("deadLetters").asLong()).isZero();
            assertThat(body.get("enqueuedTotal").asLong()).isEqualTo(3);
            assertThat(body.get("ackedTotal").asLong()).isZero();
            assertThat(body.get("deadLetteredTotal").asLong()).isZero();
            assertThat(body.get("enqueuedPerSecond").asDouble()).isGreaterThan(0);
            assertThat(body.get("ackedPerSecond").asDouble()).isZero();
        }

        @Test
        void listsDeadLetters() throws Exception {
            createQueue("{\"maxRetries\":0,\"visibilityTimeoutMs\":5000}");
            enqueue("{\"payload\":\"poison\",\"priority\":\"MEDIUM\"}");
            String messageId = json(dequeue()).get("messageId").asString();
            assertThat(json(get("/queues/" + queue + "/dlq"))).isEmpty();

            clock.advance(Duration.ofSeconds(5));
            assertThat(dequeue().statusCode()).isEqualTo(204);

            HttpResponse<String> response = get("/queues/" + queue + "/dlq");
            assertThat(response.statusCode()).isEqualTo(200);
            JsonNode deadLetters = json(response);
            assertThat(deadLetters).hasSize(1);
            assertThat(deadLetters.get(0).get("messageId").asString()).isEqualTo(messageId);
            assertThat(deadLetters.get(0).get("payload").asString()).isEqualTo("poison");
            assertThat(deadLetters.get(0).get("priority").asString()).isEqualTo("MEDIUM");
            assertThat(deadLetters.get(0).get("deliveryCount").asInt()).isEqualTo(1);
            assertThat(json(get("/queues/" + queue + "/metrics")).get("deadLetteredTotal").asLong()).isEqualTo(1);
            assertProblem(ack(messageId), 404, "no message");
        }

        @Test
        void anUnknownQueueIsNotFound() throws Exception {
            assertProblem(get("/queues/" + queue + "/metrics"), 404, "no queue named");
            assertProblem(get("/queues/" + queue + "/dlq"), 404, "no queue named");
        }

        @Test
        void prometheusCanScrapeTheMetrics() throws Exception {
            createQueue("{\"shards\":2}");
            enqueue("{\"payload\":\"a\",\"priority\":\"HIGH\"}");
            enqueue("{\"payload\":\"b\",\"priority\":\"HIGH\"}");
            String messageId = json(dequeue()).get("messageId").asString();
            ack(messageId);

            HttpResponse<String> response = get("/actuator/prometheus");

            assertThat(response.statusCode()).isEqualTo(200);
            String shard0 = "queue=\"" + queue + "\",shard=\"0\"}";
            String shard1 = "queue=\"" + queue + "\",shard=\"1\"}";
            assertThat(response.body())
                    .contains("dpq_messages_ready{priority=\"HIGH\"," + shard0 + " 0.0")
                    .contains("dpq_messages_ready{priority=\"HIGH\"," + shard1 + " 1.0")
                    .contains("dpq_messages_ready{priority=\"LOW\"," + shard1 + " 0.0")
                    .contains("dpq_messages_in_flight{" + shard0 + " 0.0")
                    .contains("dpq_oldest_message_age_seconds{" + shard1)
                    .contains("dpq_messages_enqueued_total{" + shard0 + " 1.0")
                    .contains("dpq_messages_acked_total{" + shard0 + " 1.0")
                    .contains("dpq_messages_dead_lettered_total{" + shard0 + " 0.0")
                    .contains("dpq_messages_enqueued_per_second{" + shard0)
                    .contains("dpq_messages_acked_per_second{" + shard0)
                    .contains("dpq_dead_letters{" + shard0 + " 0.0");
        }
    }

    private void createQueue(String settings) throws Exception {
        String body = "{\"name\":\"" + queue + "\"" + (settings.equals("{}") ? "}" : "," + settings.substring(1));
        assertThat(post("/queues", body).statusCode()).isEqualTo(201);
    }

    private HttpResponse<String> enqueue(String body) throws Exception {
        return post("/queues/" + queue + "/messages", body);
    }

    private HttpResponse<String> dequeue() throws Exception {
        return post("/queues/" + queue + "/messages/dequeue", "");
    }

    private HttpResponse<String> ack(String messageId) throws Exception {
        return post("/queues/" + queue + "/messages/" + messageId + "/ack", "");
    }

    private HttpResponse<String> post(String path, String body) throws Exception {
        return send(request(path).POST(HttpRequest.BodyPublishers.ofString(body)));
    }

    private HttpResponse<String> patch(String path, String body) throws Exception {
        return send(request(path).method("PATCH", HttpRequest.BodyPublishers.ofString(body)));
    }

    private HttpResponse<String> get(String path) throws Exception {
        return send(request(path).GET());
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Content-Type", "application/json");
    }

    private HttpResponse<String> send(HttpRequest.Builder request) throws Exception {
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode json(HttpResponse<String> response) {
        return JSON.readTree(response.body());
    }

    private static void assertProblem(HttpResponse<String> response, int status, String detailContains) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
        JsonNode problem = json(response);
        assertThat(problem.get("status").asInt()).isEqualTo(status);
        assertThat(problem.get("detail").asString()).contains(detailContains);
    }
}
