# API and settings

Clients use REST. Brokers talk to each other over gRPC. A client can call any broker; the broker
passes the request on to the shard's leader.

## REST API

| Operation | Endpoint | Result |
|---|---|---|
| Create queue | `POST /queues` | 201; 409 if the name exists; 400 if settings are invalid |
| Describe queue | `GET /queues/{name}` | Settings plus, per shard: leader, epoch, replicas, ISR |
| Update settings | `PATCH /queues/{name}` | 200 |
| Enqueue | `POST /queues/{name}/messages` | 201 with the message ID; 400 for an unknown priority level; 503 if not enough in-sync replicas |
| Dequeue | `POST /queues/{name}/messages/dequeue` | 200 with the message; 204 if empty |
| Acknowledge | `POST /queues/{name}/messages/{id}/ack` | 204; 404 if unknown |
| Queue metrics | `GET /queues/{name}/metrics` | JSON, aggregated across shards |
| Dead letters | `GET /queues/{name}/dlq` | The dead-lettered messages |
| Prometheus | `GET /actuator/prometheus` | Text exposition |

Request details:

- **Durations** are in milliseconds: `visibilityTimeoutMs`, `defaultTtlMs`, `ttlMs`, `waitTimeMs`.
- **Create** takes `name` plus any queue setting from the table below; a setting left out takes the broker's default.
- **Update** changes only the settings given. A `defaultTtlMs` of 0 removes the default TTL.
- **Enqueue** takes `payload`, and optionally `priority`, `shardKey`, `acks` (`all` or `leader`), and either `ttlMs` or `expiresAt`.
- **Dequeue** takes an optional `waitTimeMs` query parameter, at most 20 000. A waiting dequeue looks again every 50 ms and holds no request thread while it waits.
- **Errors** are returned as problem-detail JSON with `status` and `detail`.

Example session (`make create-queue`, `make enqueue`, `make dequeue`, `make ack ID=0-0`,
`make metrics` run the same calls):

```bash
curl -X POST localhost:8080/queues -H 'Content-Type: application/json' -d '{"name":"orders"}'
```

```bash
curl -X POST localhost:8080/queues/orders/messages -H 'Content-Type: application/json' -d '{"payload":"hello","priority":"HIGH"}'
```

```bash
curl -X POST localhost:8080/queues/orders/messages/dequeue
```

```bash
curl -X POST localhost:8080/queues/orders/messages/0-0/ack
```

```bash
curl localhost:8080/queues/orders/metrics
```

## Broker-to-broker gRPC

Brokers talk to each other over gRPC, served by a plain grpc-java server on its own port
(`dpq.advertised.grpc-port`, default 9090), with messages defined in
[broker.proto](../src/main/proto/broker.proto):

| Call | Shape |
|---|---|
| Replication fetch | One stream per shard, all sharing one connection per broker pair |
| Forwarded enqueue, dequeue, acknowledge, metrics, dead letters | Single request and response |
| Priority summaries | Single request and response |

Every call names the leader epoch the sender believes in, and the broker asked serves it only if
it leads the shard in that epoch. An error crosses the wire as the same exception the serving
broker threw: its kind travels in a gRPC header, because a status code alone cannot tell "not the
leader" from "not enough replicas". A call that fails without one of our errors means the broker
could not be reached.

## Tunable settings

| Level | Setting | Default | Trade-off it controls |
|---|---|---|---|
| Queue | `priorityLevels` | `HIGH, MEDIUM, LOW` | How finely urgency is graded vs dequeue and metrics cost |
| Queue | `visibilityTimeout` | 30 s | Fast redelivery after a crash vs duplicate work for slow consumers |
| Queue | `maxRetries` | 3 | Tolerance for flaky consumers vs how long a poison message circulates |
| Queue | `defaultTtl` | none | Whether stale work is dropped |
| Queue | `shards` | 1 | Strict ordering (1) vs throughput (many) |
| Queue | `replicationFactor` | 1 (no replication) | Failures survived vs cost and write latency |
| Queue | `minInSyncReplicas` | 1 | Durability vs availability of enqueues |
| Queue | `uncleanLeaderElection` | off | Stay available with possible loss vs go offline |
| Request | `priority` | the queue's lowest level | Per-message urgency |
| Request | `ttl` | queue's `defaultTtl` | Per-message lifetime |
| Request | `shardKey` | none | Per-key FIFO vs even spread across shards |
| Request | `acks` (`leader` or `all`) | `all` | Enqueue latency vs durability |
| Request | `waitTime` on dequeue | 0 | Long poll vs immediate empty reply |
| Broker | `failureDetectionTimeout` | 5 s | Failover speed vs false failovers |
| Broker | `replicaLagMax` | 5 s | How quickly a slow follower stops blocking writes |
| Broker | `commitTimeout` | 10 s | How long a write waits for its followers before it fails with 503 |
| Broker | `sweeperInterval` | 100 ms | Timeliness of timeouts on idle shards vs background load |
| Broker | `summaryInterval` | 200 ms | Cross-shard priority accuracy vs background load |

`priorityLevels`, `shards` and `replicationFactor` are fixed at creation. The other queue settings
can be changed later and apply to future deliveries only.

Both replica settings count the leader. With the defaults a queue is not replicated: each shard
has one copy, on one broker. Surviving a broker failure starts at `replicationFactor = 2`.

Broker settings are bound from `dpq.*` properties, or the matching environment variables
(`DPQ_BROKER_ID`, `DPQ_COORDINATOR_TYPE`, ...). The queue defaults a broker applies are under
`dpq.queue-defaults.*`. `dpq.coordinator.type` is `memory` for a single broker and `etcd` for one
broker of a cluster, with `dpq.coordinator.endpoints` naming the etcd nodes.
