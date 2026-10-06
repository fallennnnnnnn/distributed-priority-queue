# demo: producers and consumers

`QueueHarness` is a small program that loads running brokers over HTTP and then checks that no
message was lost. It uses only the REST API, like any real client would.

```
  producers ── enqueue ──▶ ┌──────────┐ ── dequeue ──▶ consumers
   (4 threads)             │ broker(s)│                 (8 threads)
                           └──────────┘ ◀── ack ─────── most of the time
                                 ▲                      (20% of acks are skipped on purpose)
                                 │
                        optionally: kill the broker that leads shard 0 half way through
```

Skipping acks is what makes it a real test: a skipped message times out, is handed out again,
and after too many tries becomes a dead letter. Killing a leader tests failover: the harness
retries requests that fail while the shard has no leader, and measures how long that took.

## Running it

Against one broker (`make run` in another terminal):

```bash
make harness
```

Against the docker-compose cluster (`make cluster` first), on a queue replicated three ways,
killing a leader half way:

```bash
make cluster-harness
```

Both are `./gradlew harness --args="..."` underneath; the options are below.

## What it checks

At the end it prints the counts, the latencies and one verdict line. It passes when:

- every enqueue a broker confirmed was delivered at least once (**lost** must be 0),
- every message ended up acknowledged, dead-lettered or expired,
- nothing is left waiting or in flight.

It also reports how many messages were delivered more than once, how many requests it had to
retry, and, after a kill, how long the shard was without a leader. It prints the p50, p95 and
p99 latency of enqueue, dequeue and ack.

A retried enqueue whose first attempt was committed but not answered leaves a duplicate: the
brokers count one message more than the harness produced and one message is delivered twice.
That is what at-least-once permits, and the report shows it rather than hiding it.

## Options

Each option is given as `--name=value`.

| Option | Default | Meaning |
|---|---|---|
| `urls` | `http://localhost:8080` | The brokers to load, comma-separated. Requests are spread over them. |
| `producers` | 4 | Producer threads. |
| `consumers` | 8 | Consumer threads. |
| `messages` | 2000 | Messages to enqueue in total. |
| `skipAckPercent` | 20 | How often a consumer skips the ack. |
| `shards` | 3 | Shards of the queue it creates. |
| `replicationFactor` | 1 | Replicas per shard. Needs at least that many brokers. |
| `minInSyncReplicas` | 1 | In-sync replicas an enqueue needs. |
| `visibilityTimeoutMs` | 1000 | How long a message stays hidden after a dequeue. |
| `maxRetries` | 3 | Redeliveries before a message becomes a dead letter. |
| `timeoutSeconds` | 120 | How long to wait for every message to settle. |
| `killCommand` | none | A shell command run half way through, with `{leader}` replaced by the ID of the broker leading shard 0, for example `docker compose kill {leader}`. |

Each run creates its own queue, named `harness-` followed by the current time.
