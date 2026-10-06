# Running, deploying and observing

The [Makefile](../Makefile) wraps every command here; `make help` lists the targets.

## One broker

Needs JDK 17. Start a broker on port 8080:

```bash
make run
```

In another terminal, run producers and consumers against it. They enqueue 2000 messages, skip
20% of the acknowledgements, and check at the end that every message was acknowledged or
dead-lettered:

```bash
make harness
```

## A cluster

Needs Docker. This builds the image and starts three brokers and one etcd; the brokers' REST
APIs are on ports 8081, 8082 and 8083:

```bash
make cluster
```

Then run the harness against all three brokers, on a queue replicated three ways, and have it
kill the broker that leads shard 0 half way through:

```bash
make cluster-harness
```

Bring the killed broker back, and it catches up and rejoins as a follower:

```bash
make cluster-restart BROKER=broker-1
```

`make monitoring` also starts Prometheus on port 9090, scraping all three brokers.
`make cluster-logs` follows the brokers' logs and `make cluster-down` stops everything.

## By hand

`make create-queue`, `make enqueue`, `make dequeue`, `make ack ID=0-0` and `make metrics` send
the requests in [api.md](api.md#rest-api) with curl. `HOST=localhost:8081` and `QUEUE=orders`
pick the broker and the queue.

## Tests

```bash
make test
```

The tests need only a JDK, except the etcd coordinator tests, which start etcd in a container
and are skipped when Docker is not running.

## Deployment stacks

Every stack runs the same code. The `dpq.coordinator.type` setting picks the kind of broker:
`memory` is a single broker with its coordinator inside the process, and `etcd` is one broker of
a cluster, which also starts its gRPC server.

| Stack | What runs | Coordinator | Broker-to-broker | Used for |
|---|---|---|---|---|
| Single broker | One JVM via `make run` | In-memory | None (a cluster of one) | Development; meeting the original spec; reviewers without Docker |
| In-process test cluster | Several brokers inside one test JVM | In-memory | A network inside the JVM that tests can hold, partition and delay | Replication and failover tests |
| gRPC test cluster | Several brokers inside one test JVM, each with a gRPC server on a localhost port | In-memory | gRPC | Proving the cluster behaves the same over the real transport |
| etcd in a container | The coordinator alone | etcd | None | The coordinator contract tests |
| docker-compose cluster | One etcd node, three brokers, optional Prometheus | etcd | gRPC | The demo: load, kill a leader, show failover |
| Production (described, not built) | Three or five etcd nodes, brokers with stable identities, a load balancer on the HTTP port, Prometheus | etcd | gRPC | Where this would go next |

- **What each needs:** the first three need only a JDK. The etcd tests and the compose cluster need Docker.
- **Defaults work everywhere:** a queue created with default settings is not replicated, so it works on every stack, including a single broker. The compose demo creates its queue with `replicationFactor = 3` and `minInSyncReplicas = 2`.

The compose file publishes each broker's REST port on the host and gives each broker its ID,
advertised host and the etcd endpoint through `DPQ_*` environment variables. The image is built
by the [Dockerfile](../Dockerfile) in two stages, so Docker needs no JDK on the host.

## What a run shows

Measured on one laptop (Apple silicon), with everything on the same machine, so read the
latencies as "comfortably inside the target", not as a benchmark. Each run used 4 producers and
8 consumers.

| Run | Messages | Lost | Enqueue p95 | Dequeue p95 | Ack p95 |
|---|---|---|---|---|---|
| One broker, 20% of acks skipped | 2000 | 0 | 3.1 ms | 2.9 ms | 2.5 ms |
| Three brokers, replicated three ways | 5000 | 0 | 9.7 ms | 9.5 ms | 8.9 ms |
| Three brokers, leader killed half way | 2000 | 0 | 44.8 ms | 44.3 ms | 42.3 ms |

In the failover run the harness killed broker-1, which led shard 0:

- **No confirmed message was lost.** All 2000 confirmed enqueues were delivered and acknowledged.
- **One duplicate.** An enqueue whose answer was lost in the failover was sent again, so the brokers counted 2001 messages and one was delivered twice. That is what at-least-once permits.
- **Five seconds without a leader.** Shard 0 was leaderless for 5.0 s, which is the failure-detection timeout: the time it takes the dead broker's lease to run out. The other two shards kept serving.
- **The killed broker rejoined.** Restarted, it came back with an empty log, caught up, and was added back to every in-sync set.

## Metrics

The metrics the spec asks for:

| ID | Metric | Definition |
|---|---|---|
| M-1 | Oldest message age | Per queue: age in seconds of the oldest message that is neither expired nor in flight. |
| M-2 | Message count | Messages ready to be consumed, broken down by priority level (and by queue name). |
| M-3 | Messages in flight | Per queue: messages dequeued but not yet acknowledged. |
| M-4 | Throughput | Per queue: messages enqueued per second and messages acknowledged per second. |
| M-5 | Failures / DLQ | Per queue: count of messages moved to the dead-letter queue. |

Each broker exports these at `/actuator/prometheus` for the shards it leads, labelled with
`queue` and `shard`. Prometheus adds them up per queue with `sum by (queue)`, and takes
`max by (queue)` for the oldest message age.

| Prometheus metric | Type | Covers |
|---|---|---|
| `dpq_oldest_message_age_seconds` | gauge | M-1 |
| `dpq_messages_ready`, also labelled with `priority` | gauge | M-2 |
| `dpq_messages_in_flight` | gauge | M-3 |
| `dpq_messages_enqueued_total`, `dpq_messages_acked_total` | counter | M-4, through `rate()` |
| `dpq_messages_enqueued_per_second`, `dpq_messages_acked_per_second` | gauge | M-4, averaged over the last 60 seconds |
| `dpq_messages_dead_lettered_total` | counter | M-5 |
| `dpq_dead_letters` | gauge | Dead letters currently held |
| `dpq_messages_delivered_total`, `dpq_messages_redelivered_total`, `dpq_messages_expired_total` | counter | Deliveries, redeliveries and TTL expiries |

`GET /queues/{name}/metrics` returns the same numbers as JSON, already summed over the queue's shards.

The cluster adds replication metrics, published by a shard's leader like the ones above:

| Prometheus metric | Type | Meaning |
|---|---|---|
| `dpq_in_sync_replicas` | gauge | Replicas that have every committed event |
| `dpq_under_replicated` | gauge | 1 when the in-sync set is smaller than the replication factor |
| `dpq_leader_epoch` | gauge | Goes up by one with every new leader |
| `dpq_log_end_offset`, `dpq_high_watermark` | gauge | The gap between them is what is waiting for followers |
| `dpq_leader_elections_total` | counter | Elections this broker won for the shard |
| `dpq_shard_resets_total` | counter | Elections won without being in sync, which may have lost messages |

A broker publishes a shard's metrics while it leads the shard and withdraws them when it stops.
A new leader has replayed the same events, so its totals carry on from the old leader's.
