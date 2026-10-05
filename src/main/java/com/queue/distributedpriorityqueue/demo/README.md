# demo: producers and consumers

`QueueHarness` is a small program that loads a running broker over HTTP and then checks that no
message was lost. It uses only the REST API, like any real client would.

```
  producers ── enqueue ──▶ ┌────────┐ ── dequeue ──▶ consumers
   (4 threads)             │ broker │                 (8 threads)
                           └────────┘ ◀── ack ─────── most of the time
                                                      (20% of acks are skipped on purpose)
```

Skipping acks is what makes it a real test: a skipped message times out, is handed out again,
and after too many tries becomes a dead letter.

## Running it

Start a broker in one terminal:

```bash
./gradlew bootRun
```

Run the harness in another:

```bash
./gradlew harness --args="--url=http://localhost:8080"
```

## What it checks

At the end it prints the counts and one verdict line. It passes when:

- every message it produced was enqueued and delivered at least once,
- every message ended up acknowledged, dead-lettered or expired,
- nothing is left waiting or in flight.

It also prints the p50, p95 and p99 latency of enqueue, dequeue and ack.

## Options

Each option is given as `--name=value`.

| Option | Default | Meaning |
|---|---|---|
| `url` | `http://localhost:8080` | The broker to load. |
| `producers` | 4 | Producer threads. |
| `consumers` | 8 | Consumer threads. |
| `messages` | 2000 | Messages to enqueue in total. |
| `skipAckPercent` | 20 | How often a consumer skips the ack. |
| `shards` | 3 | Shards of the queue it creates. |
| `visibilityTimeoutMs` | 1000 | How long a message stays hidden after a dequeue. |
| `maxRetries` | 3 | Redeliveries before a message becomes a dead letter. |
| `timeoutSeconds` | 120 | How long to wait for every message to settle. |

Each run creates its own queue, named `harness-` followed by the current time.
