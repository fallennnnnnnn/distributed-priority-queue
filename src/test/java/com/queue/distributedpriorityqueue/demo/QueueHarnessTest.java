package com.queue.distributedpriorityqueue.demo;

import static org.assertj.core.api.Assertions.assertThat;

import com.queue.distributedpriorityqueue.demo.QueueHarness.Options;
import com.queue.distributedpriorityqueue.demo.QueueHarness.Report;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

/** Runs the producer and consumer harness against a broker on a random port, on the real clock. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "dpq.advertised.http-port=8080")
class QueueHarnessTest {

    @Value("${local.server.port}")
    private int port;

    @Test
    @Timeout(120)
    void nothingIsLostWhenSomeConsumersSkipTheirAcks() throws Exception {
        Options options = new Options(URI.create("http://localhost:" + port), 3, 6, 600, 30, 3,
                Duration.ofMillis(300), 2, Duration.ofSeconds(90));

        Report report = QueueHarness.run(options);

        assertThat(report.passed()).as(report.describe()).isTrue();
        assertThat(report.deliveries()).as("skipped acks cause redeliveries").isGreaterThan(report.produced());
        assertThat(report.acked() + report.deadLettered()).isEqualTo(600);
    }

    @Test
    @Timeout(120)
    void everyMessageIsAckedOnceWhenNoAckIsSkipped() throws Exception {
        Options options = new Options(URI.create("http://localhost:" + port), 3, 6, 600, 0, 3,
                Duration.ofSeconds(30), 2, Duration.ofSeconds(90));

        Report report = QueueHarness.run(options);

        assertThat(report.passed()).as(report.describe()).isTrue();
        assertThat(report.acked()).isEqualTo(600);
        assertThat(report.deliveries()).isEqualTo(600);
        assertThat(report.deadLettered()).isZero();
    }
}
