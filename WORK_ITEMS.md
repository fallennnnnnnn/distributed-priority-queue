# Work items

Implementation plan for the design in [README.md](README.md). The sections it mentions now live under [docs/](docs/).

- `[ ]` not started, `[x]` done.
- Size is relative effort, not a time estimate: **S** small, **M** medium, **L** large.
- Each phase ends with a working, tested service. A phase starts only after the previous exit
  check passes.
- **Phase 2 is the point where the original assignment is fully met on one broker.** Phases 3 to 5
  add the cluster.

## Phase 0 — Project setup

- [x] **0.1** Initialise the git repository. (S)
- [x] **0.2** Dependencies: web, validation, actuator, Prometheus registry, etcd client (jetcd), grpc-java and the protobuf Gradle plugin; for tests, Awaitility and Testcontainers. gRPC and protobuf versions come from the Spring Boot BOM, which also moves the gRPC that jetcd brings onto the same version. *Done when* `./gradlew build` passes. (S)
- [x] **0.3** Package layout: `core`, `log`, `replication`, `coordination`, `cluster`, `transport`, `api`, `metrics`, `demo`, `exception`. (S)
- [x] **0.4** Broker settings (`dpq.*`): broker ID, advertised address (HTTP and gRPC ports), coordinator type (`memory` or `etcd`) and endpoints, queue defaults, the broker settings from docs/api.md. Validated at startup. (S)
- [x] **0.5** Injectable clock for all time-dependent code, plus a manual clock for tests. (S)

## Phase 1 — Shard state machine and log (one replica)

- [x] **1.1** Domain types: per-queue priority levels (ordered names, 1 to 10, default `HIGH, MEDIUM, LOW`), `Message`, `MessageId` (`{shard}-{offset}`), `QueueConfig` with validation and the defaults from docs/api.md (one shard, no replication). (S)
- [x] **1.2** Events: `Enqueued`, `Delivered`, `Acked`, `Requeued`, `DeadLettered`, `Expired`, as plain records. Their wire format comes with the gRPC transport in 5.3. (S)
- [x] **1.3** `ShardStateMachine.apply(event)`: one lane per priority level, ID map, deadline index, TTL index, dead letters, counters. (M)
- [x] **1.4** Decision methods that produce events: `enqueue`, `dequeue(now)`, `ack`, `reclaim(now)`. (M)
- [x] **1.5** `ShardLog` interface and `InMemoryShardLog`: append, read from an offset, truncate, epoch per entry, end offset of an epoch, high watermark. (M)
- [x] **1.6** `ShardReplica` leader path: lock, then decide, append, apply; returns a future that completes when the event is committed (immediately with one replica). (M)
- [x] **1.7** Metrics snapshot: ready count by priority, in flight, oldest message age, dead letters, counters, 60-second rates. (S)
- [x] **1.8** Unit tests on the manual clock (M):
  - priority order; FIFO within a priority
  - custom priority levels: one level behaves as plain FIFO; five levels are served in the declared order; no priority means the lowest level; an unknown level is rejected
  - an in-flight message is invisible; it returns after the timeout to its original position
  - delivery count; dead-lettered after `maxRetries + 1` deliveries; `maxRetries = 0`
  - TTL: expired message not delivered; expired in-flight message can still be acked; not redelivered once expired
  - late ack accepted (message back in ready, or redelivered to another consumer)
  - ack is idempotent; ack of an unknown ID, a never-delivered message, a dead-lettered message
  - oldest message age ignores in-flight and expired messages
- [x] **1.9** Replay test: a recorded event sequence applied to a fresh state machine gives identical state. (S)
- [x] **1.10** Concurrency tests on one shard, repeated runs (M):
  - no message held by two consumers at once
  - `enqueued = ready + in flight + acked + dead-lettered + expired`
  - consumers that always ack: every message acked exactly once
  - consumers that sometimes skip the ack: every message ends acked or dead-lettered
  - ack racing the visibility timeout

**Exit:** FR-3 to FR-13 hold on one shard, in process. Creating and configuring queues through the service (FR-1, FR-2) comes with the registry in phase 2.

## Phase 2 — Single-broker service: sharding, API, metrics

- [x] **2.1** `Coordinator` interface and `InMemoryCoordinator`: brokers with incarnations and leases that tests can expire, queue configs, versioned shard state with compare-and-swap, watches. A single broker runs as a cluster of one. (M)
- [x] **2.2** Queue creation: validate settings, assign replicas, write config and shard states in one transaction; brokers create local replicas from watched metadata. (M)
- [x] **2.3** Router: enqueue shard choice (`shardKey` hash or round-robin), ack routing by message ID, dequeue shard order by highest ready priority. (M)
- [x] **2.4** Sweeper thread that runs `reclaim` on led shards every `sweeperInterval`. (S)
- [x] **2.5** Public REST API: create, describe, update settings, enqueue, dequeue, ack, dead-letter listing; request validation; error mapping (400, 404, 409, 503). (M)
- [x] **2.6** `waitTime` long poll on dequeue. (S)
- [x] **2.7** Metrics: Micrometer binder labelled by queue, shard and priority; JSON `GET /queues/{name}/metrics`. (M)
- [x] **2.8** Sharding tests: same `shardKey` lands on one shard and keeps FIFO; round-robin spreads evenly; `shards = 1` is strictly ordered; a HIGH message in any shard is delivered before a LOW one. (S)
- [x] **2.9** HTTP tests for every endpoint and error code. (M)
- [x] **2.10** Producer / consumer harness against one broker: concurrent producers and consumers, some consumers skipping acks; prints the invariants and metrics. (M)
- [x] **2.11** Single-broker stack: `./gradlew bootRun` starts one broker with the in-memory coordinator and no other process, and a queue created with default settings works on it. *Done when* the harness from 2.10 passes against it. (S)

**Exit:** every FR, metric and deliverable in the original spec is met by one broker.

## Phase 3 — Replication (several brokers in one process)

- [x] **3.1** `BrokerClient` interface for broker-to-broker calls (fetch stream, forwarded requests, priority summaries), with an in-process transport that can drop, delay and partition. (M)
- [x] **3.2** In-process test cluster: N brokers sharing one `InMemoryCoordinator`; start, stop and partition brokers; controllable clock. (M)
- [x] **3.3** Fetch protocol as a stream per shard. The follower opens it with shard, replica incarnation, epoch, fetch offset and last entry epoch. The leader sends entries and the high watermark as events are appended; the follower reports its log end. `NOT_LEADER`, `FENCED_EPOCH` or `DIVERGED` ends the stream; `DIVERGED` says where the follower's last epoch ends in the leader's log. (M)
- [x] **3.4** Follower: keep the fetch stream open and reopen it when it ends; append, apply up to the high watermark; on `DIVERGED`, truncate to where the two logs last agree. A follower applies only committed entries, so its state needs no rebuild; that is needed when a leader steps down (4.1). (L)
- [x] **3.5** Leader: track each follower's progress; high watermark = lowest log end in the ISR; complete commit futures; commit timeout returns 503. (M)
- [x] **3.6** ISR shrink after `replicaLagMax` and expand on catch-up, through coordinator compare-and-swap. The new set is used only after the write succeeds. (M)
- [x] **3.7** Enqueue checks: 503 when ISR is below `minInSyncReplicas`; `acks=leader` and `acks=all`. (S)
- [x] **3.8** Replica assignment across brokers at queue creation; reject a `replicationFactor` larger than the number of live brokers. (S)
- [x] **3.9** Forward requests to the shard leader; on `NOT_LEADER`, refresh metadata and retry once. (M)
- [x] **3.10** Priority summaries exchanged between brokers every `summaryInterval`; dequeue routing uses them. (M)
- [x] **3.11** JSON metrics and dead-letter listing gather results from all shard leaders. (S)
- [x] **3.12** Replication tests (L):
  - an `acks=all` enqueue returns only after every ISR member has the event
  - a follower's state equals the leader's after catch-up
  - a slow follower leaves the ISR after `replicaLagMax` and writes continue
  - ISR below `minInSyncReplicas`: enqueue returns 503, dequeue and ack still work
  - a recovered follower is added back; a new follower catches up from empty
  - `acks=leader` returns before replication
  - a queue with default settings has one replica and commits without waiting for any follower

**Exit:** with a fixed leader, any follower can be stopped or slowed without losing or blocking committed work.

## Phase 4 — Leader election and failover

- [x] **4.1** Role changes driven by watched shard state. Become leader: apply the log tail, reset follower progress. Become follower: stop serving, fail pending requests, start fetching. (L)
- [x] **4.2** Election: on a shard with no live leader, ISR members (matched by incarnation) try one compare-and-swap: leader = self, epoch + 1, ISR minus the dead leader. (M)
- [x] **4.3** Fencing: epoch on every internal request, lower epochs rejected; a leader stops serving when its lease is lost, then resumes or steps down after re-registering. (M)
- [x] **4.4** Broker restart: new incarnation, empty log, catch up as follower, added back to the ISR. (M)
- [x] **4.5** `uncleanLeaderElection`. A shard with `replicationFactor = 1` always recovers this way: its restarted broker takes over with an empty log, and the reset is counted. (S)
- [x] **4.6** Replication metrics: ISR size, under-replicated shards, epoch, log end, high watermark, elections, shard resets. (S)
- [x] **4.7** Failover tests, one per row of the failure table in docs/guarantees.md (L):
  - leader killed under load: no `acks=all` enqueue is lost; each ends acked or dead-lettered
  - the new leader was in the ISR and the epoch went up by one
  - in-flight messages keep deadlines and delivery counts; an acked message is never redelivered
  - a partitioned old leader cannot commit and its requests are rejected by epoch
  - a diverged follower truncates only uncommitted entries, including the case where its own high watermark lags
  - a restarted broker cannot be elected until it has caught up
  - all ISR members lost: shard offline, or elected with loss when unclean election is on
  - an unreplicated shard whose broker restarts: the broker takes over again with an empty log and the reset is counted
  - coordinator outage: brokers stop at lease expiry and resume afterwards with no data lost
  - two replicas racing an election: exactly one wins

**Exit:** every row of the failure table in docs/guarantees.md has a passing test.

## Phase 5 — etcd and deployment

- [x] **5.1** `EtcdCoordinator` using jetcd: key layout, lease keep-alive, transactions on mod revision, prefix watches. (M)
- [x] **5.2** Coordinator contract tests run against both `InMemoryCoordinator` and etcd in a container. (M)
- [x] **5.3** gRPC transport for `BrokerClient`: protobuf definitions for events and broker-to-broker calls; a plain grpc-java server on its own port; fetch as a two-way stream per shard, forwarding and summaries as single calls; reconnect on failure. *Done when* replication, passing requests on and failover also pass over gRPC on localhost. The tests that hold or partition the network stay on the in-process transport, which is the one that can do that. (L)
- [x] **5.4** Dockerfile and docker-compose: etcd, three brokers (each with an HTTP and a gRPC port), optional Prometheus. (M)
- [x] **5.5** Harness against the cluster: create the queue with `replicationFactor = 3` and `minInSyncReplicas = 2`, run load, kill a shard leader mid-run, report lost, duplicated and acked counts and the failover time. (M)
- [x] **5.6** Latency check: p95 enqueue and dequeue from the harness, recorded in docs/operations.md. (S)

**Exit:** `docker compose up` plus the demo shows a failover with no committed message lost.

## Phase 6 — README and submission

- [x] **6.1** README: how to run each deployment stack, test methodology, measured results, guarantees, limits, what we would do with more time. (M)
- [x] **6.2** Review pass: every setting in docs/api.md exists in code and has a test; every FR and NFR maps to a test. (S)

## Not planned

Listed in docs/decisions.md: log on disk, log trimming and snapshots, changing shard count, leader
rebalancing and replica reassignment, idempotent enqueue, negative acknowledge and visibility
extension, dead-letter redrive, priority aging.
