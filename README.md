# Distributed Priority Queue Service

> **Status:** design settled, implementation not started. This README describes the design being
> built. Progress is tracked in [WORK_ITEMS.md](WORK_ITEMS.md). Run instructions and measured
> results will be added as the phases land.

## 1. Problem summary

Build the **Queue Service**: a priority queue that multiple backend services share to enqueue and
process work items. The service owns queues and the full message lifecycle (enqueue, dequeue,
acknowledge, retry, dead-letter) and exposes operational metrics.

Three things define it:

- **Priority ordering** — consumers get the highest-priority available message, FIFO within a priority.
- **At-least-once delivery** — a dequeued message stays hidden ("in flight") until it is acknowledged;
  if the consumer does not acknowledge in time, the message is redelivered, and after too many
  redeliveries it is moved to a dead-letter queue.
- **Safe under concurrency** — many producers and consumers call the service at the same time.

## 2. Scope

The spec requires an in-memory, single-node service plus a written scaling story. We chose to build
the scaling story instead of only describing it (see section 10 for why and what it costs).

| In scope | Out of scope |
|---|---|
| The Queue Service and its REST API | Real producer / consumer services (test harnesses are enough) |
| A multi-broker cluster: sharded queues, replicated shards, leader failover | The monitoring system itself (we only expose the data) |
| Tunable settings per queue, per request and per broker | Disk persistence, log trimming, changing a queue's shard count (section 12) |
| Tests, including concurrency and failover tests | |
| Producer / consumer harness and a failover demo | |
| docker-compose with three brokers and etcd | |

## 3. Message lifecycle

```
                 enqueue
                    │
                    ▼
   ┌──────────►   READY   ──── TTL elapsed ──────►  EXPIRED (never delivered)
   │                │
   │             dequeue
   │                ▼
   │            IN FLIGHT ──── acknowledge ──────►  ACKED (permanently removed)
   │                │
   │       visibility timeout
   │                │
   └─ retries left ─┴─ retry limit exceeded ─────►  DEAD-LETTER QUEUE
```

## 4. Functional requirements

From the spec:

| ID | Area | Requirement |
|---|---|---|
| FR-1 | Admin | Create a named queue. |
| FR-2 | Admin | Configure per-queue settings: visibility timeout and retry limit. |
| FR-3 | Enqueue | Accept a message with a payload (opaque blob/string), a priority (`HIGH` / `MEDIUM` / `LOW`) and an optional TTL or expiration time. |
| FR-4 | Enqueue | Return a unique message ID. |
| FR-5 | Enqueue | A message whose TTL has elapsed is not delivered. |
| FR-6 | Dequeue | Return the highest-priority available message. |
| FR-7 | Dequeue | Within the same priority, deliver in FIFO order. |
| FR-8 | Dequeue | Mark the dequeued message "in flight": it is not visible to other consumers until it is acknowledged or its visibility timeout expires. |
| FR-9 | Acknowledge | Acknowledge successful processing of a message by its ID. |
| FR-10 | Acknowledge | An acknowledged message is permanently removed and never redelivered. |
| FR-11 | Retry | A message not acknowledged within the visibility timeout becomes visible again and is redelivered. |
| FR-12 | Retry | Track how many times each message has been redelivered. |
| FR-13 | Dead-letter | When the redelivery count exceeds the queue's configured limit, move the message to a dead-letter queue instead of redelivering it. |
| FR-14 | Metrics | Expose the queue-level metrics in section 9 in a form a system like Prometheus can scrape. |

Added by our design, beyond the spec:

| ID | Area | Requirement |
|---|---|---|
| EX-1 | Priority levels | The client defines the queue's priority levels when creating it. `HIGH` / `MEDIUM` / `LOW` is the default. |
| EX-2 | Sharding | A queue is split into a configurable number of shards so load spreads across brokers. |
| EX-3 | Replication | Each shard is replicated; the client sets the replication factor and the minimum number of in-sync replicas. |
| EX-4 | Failover | When a shard's leader fails, an in-sync replica takes over without losing committed messages. |
| EX-5 | Describe | A client can read each shard's leader, replicas and current in-sync replicas. |
| EX-6 | Tunability | Clients choose their own trade-offs through the settings in section 7. |

## 5. Non-functional requirements

| ID | Requirement | Approach |
|---|---|---|
| NFR-1 | **Thread safety (mandatory).** Correct with multiple concurrent producers and consumers. | One lock per shard; concurrency tests assert invariants |
| NFR-2 | **At-least-once delivery.** A message is delivered until it is acknowledged, dead-lettered or expired. | Visibility timeout and redelivery; every transition replicated before the caller is answered |
| NFR-3 | **Horizontal scalability.** The architecture must extend to multiple instances of the service. | Built: shards spread across brokers |
| NFR-4 | **Latency.** p95 < 100 ms for enqueue and dequeue within a region (design target). | One replication round trip per write; measured by the harness |
| NFR-5 | **Durability.** Messages are not silently lost. | Built: replication to in-sync replicas, chosen per queue; the default is no replication (section 8 states the limits) |
| NFR-6 | **Node failure.** Handle one queue instance going down. | Built: for replicated queues, leader election from the in-sync replicas |

## 6. Design

### 6.1 Overview

```
        Producers / Consumers                    Prometheus
                 │  call any broker                  │  scrapes every broker
   ┌─────────────┼─────────────┐
   ▼             ▼             ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│ Broker 1 │ │ Broker 2 │ │ Broker 3 │    example: one queue, three shards,
│ S0 LEAD  │ │ S0 foll. │ │ S0 foll. │    replication factor three
│ S1 foll. │ │ S1 LEAD  │ │ S1 foll. │
│ S2 foll. │ │ S2 foll. │ │ S2 LEAD  │    followers stream from leaders (gRPC)
└────┬─────┘ └────┬─────┘ └────┬─────┘
     └────────────┼────────────┘
                  ▼
        ┌────────────────────┐
        │ Coordinator (etcd) │   broker leases; per shard: leader, epoch, in-sync replicas
        └────────────────────┘
```

Inside a broker, a request passes through four layers:

```
REST API ──▶ Router ──▶ Shard replica (leader) ──▶ log ──▶ state machine
               └─ forwards to another broker when that broker leads the shard
```

| Term | Meaning |
|---|---|
| Broker | One instance of the service. |
| Shard | One slice of a queue, with its own log, state and lock. |
| Replica | A broker's copy of a shard. One replica is the leader, the rest are followers. |
| In-sync replicas (ISR) | The replicas that have every committed event. Only they can become leader. |
| Epoch | A counter that goes up each time a shard gets a new leader. Used to reject stale leaders. |
| High watermark | The log position up to which every in-sync replica has the events. Events before it are committed. |
| Incarnation | An ID a broker generates when its process starts. A restarted broker is a new incarnation. |

### 6.2 Priority levels

The client defines a queue's priority levels when creating it: an ordered list of names, highest
first, with between 1 and 10 levels. The default is `HIGH, MEDIUM, LOW`, which is what the spec
asks for. Each enqueue names one of the queue's levels; an enqueue with no priority gets the
lowest level, and an unknown level is rejected. Level names are matched ignoring case. The levels
are fixed once the queue exists.

Why per queue instead of a fixed three:

- **Fits the workload.** A queue can have two levels (`urgent`, `normal`) or five. No one has to squeeze their urgency classes into three.
- **One level is a plain FIFO queue.** The same service covers work that has no priorities at all.
- **Adding a level needs no code change.** A level is data: each one is a lane in the shard, and dequeue walks the lanes in order.
- **Metrics in the client's own terms.** Ready counts are labelled with the queue's own level names.
- **Queues are independent.** Each queue picks its own scheme without affecting the others.

The cost is that each level adds a lane to check on dequeue, a label value in the metrics and an
entry in the priority summaries brokers exchange. The cap of 10 keeps those bounded.

### 6.3 Shard state machine

All queue behaviour lives in one class, the shard state machine. Its state:

- **Ready lanes** — one sorted map per priority level, in the queue's order, keyed by log offset. Dequeue takes the first entry of the highest non-empty lane.
- **In-flight messages** — a map by ID plus an index ordered by visibility deadline.
- **TTL index** — ordered by expiry time, so expired messages are removed exactly.
- **Dead letters and counters.**

State changes only by applying events from the shard's log:
`Enqueued`, `Delivered`, `Acked`, `Requeued`, `DeadLettered`, `Expired`.

The leader turns requests into events while holding the shard lock:

```
enqueue   append Enqueued; the message enters its priority lane
dequeue   reclaim(now), then append Delivered for the first message of the highest
          non-empty lane (delivery count + 1, deadline = now + visibilityTimeout)
ack       append Acked if the message exists and has been delivered at least once
reclaim(now)
          each in-flight message past its deadline:
              TTL elapsed                   -> Expired
              delivery count > maxRetries   -> DeadLettered
              otherwise                     -> Requeued (back to its original position)
          each ready message past its TTL   -> Expired
```

`reclaim` runs at the start of every dequeue and on a background sweeper, so timeouts are exact
for active shards and still happen on idle ones. The leader stamps every time value into the
event, so followers apply events without reading their own clocks.

### 6.4 Sharding

- **Enqueue routing:** an optional `shardKey` hashes to a shard, so messages with the same key keep their order. Without a key, enqueues go round-robin.
- **Message ID:** `{shard}-{offset}`. An acknowledge routes to the right shard without a lookup.
- **Dequeue routing:** brokers exchange a summary of each shard's ready counts by priority level every 200 ms. A dequeue goes to the shard with the highest ready priority and falls through to the next shard if that one is empty.
- **Any broker:** a client can call any broker; the broker forwards to the shard leader.

### 6.5 Replication

Each shard is a log of events. The leader appends. Each follower keeps a gRPC stream open to the
leader: it states its log position, the leader sends new events as they are appended, and the
follower reports how far it has got. Followers append the same events in the same order.

```
offset          0    1    2    3    4    5    6    7
Leader    B1   enq  enq  dlv  ack  enq  dlv  enq  ack
Follower  B2   enq  enq  dlv  ack  enq  dlv  enq
Follower  B3   enq  enq  dlv  ack  enq  dlv
                                             ▲
                         high watermark = 6: offsets 0–5 are on every
                         in-sync replica and are committed
```

- **Commit rule:** the leader answers a caller only when that caller's event is below the high watermark. This holds for enqueue, dequeue and acknowledge.
- **Locking:** the leader decides, appends and applies under the shard lock, then waits for the commit outside the lock.
- **Followers** apply events only up to the high watermark.
- **ISR upkeep:** the leader removes a follower that has not caught up within `replicaLagMax` and adds it back once it has. The leader writes each change to the coordinator first and uses the new set only after that write succeeds.
- **Minimum ISR:** when the ISR is smaller than `minInSyncReplicas`, enqueues are rejected with 503. Dequeue and acknowledge keep working so consumers can drain the queue.
- **`acks`:** an enqueue with `acks=all` (default) waits for the commit. With `acks=leader` it returns once the leader has appended, which is faster and can lose the message if the leader fails right after.

### 6.6 Coordinator, election and fencing

What the coordinator stores:

```
/brokers/{id}                      -> address, incarnation   (tied to a lease; vanishes when the broker dies)
/queues/{name}/config              -> queue settings
/queues/{name}/shards/{n}/state    -> { leader, epoch, isr, replicas }
```

- **Assignment:** on queue creation, the broker handling the request spreads each shard's replicas round-robin across live brokers and writes the config and all shard states in one transaction.
- **Liveness:** each broker keeps a lease alive. When a broker dies, its key disappears after the failure-detection timeout.
- **Election:** when a shard has no live leader, each in-sync replica tries one compare-and-swap on the shard state: leader = me, epoch + 1, ISR minus the dead leader. The coordinator lets exactly one succeed. There is no separate controller broker.
- **Eligibility:** ISR members are recorded by broker incarnation. A restarted broker has an empty log and a new incarnation, so it cannot be elected until it has caught up and been re-added.
- **Fencing:** every broker-to-broker request carries the epoch and replicas reject a lower one. A leader that loses its lease stops serving.
- **After a leader change:** log entries carry the epoch that wrote them. A follower whose log diverges from the new leader's is told where the two logs last agree, truncates to that point and refetches. It then rebuilds its state by replaying its log.
- **No in-sync replica alive:** the shard goes offline. With `uncleanLeaderElection` on, any live replica may take over, accepting that committed messages may be lost.
- **Unreplicated shards:** a shard with `replicationFactor = 1` always recovers the unclean way. Its broker comes back with an empty log and takes over again, since there is no other copy to wait for. Each such reset is counted in a metric.

### 6.7 API

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

Clients use REST. Brokers talk to each other over gRPC, served by a plain grpc-java server on its
own port, with messages defined in protobuf:

| Call | Shape |
|---|---|
| Replication fetch | One stream per shard, all sharing one connection per broker pair |
| Forwarded enqueue, dequeue, acknowledge | Single request and response |
| Priority summaries | Single request and response |

### 6.8 Deployment stacks

Every stack runs the same jar. The `dpq.coordinator` setting picks the coordinator: `memory` or `etcd`.

| Stack | What runs | Coordinator | Broker-to-broker | Used for |
|---|---|---|---|---|
| Single broker | One JVM via `./gradlew bootRun` | In-memory | None (a cluster of one) | Development; meeting the original spec; reviewers without Docker |
| In-process test cluster | Several brokers inside one test JVM | In-memory | In-process transport that can drop, delay and partition | Replication and failover tests |
| Integration tests | Brokers on localhost ports, etcd in a Testcontainers container | etcd | gRPC | Coordinator contract tests; the gRPC transport |
| docker-compose cluster | One etcd node, three brokers, optional Prometheus | etcd | gRPC | The demo: load, kill a leader, show failover |
| Production (described, not built) | Three or five etcd nodes, brokers with stable identities, a load balancer on the HTTP port, Prometheus | etcd | gRPC | The scaling story |

- **What each needs:** the first two need only a JDK. The integration tests and the compose cluster need Docker.
- **Defaults work everywhere:** a queue created with default settings is not replicated, so it works on every stack, including a single broker. The compose demo creates its queue with `replicationFactor = 3` and `minInSyncReplicas = 2`.

## 7. Tunable settings

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
| Broker | `sweeperInterval` | 100 ms | Timeliness of timeouts on idle shards vs background load |
| Broker | `summaryInterval` | 200 ms | Cross-shard priority accuracy vs background load |

`priorityLevels`, `shards` and `replicationFactor` are fixed at creation. The other queue settings
can be changed later and apply to future deliveries only.

Both replica settings count the leader. With the defaults a queue is not replicated: each shard
has one copy, on one broker. Surviving a broker failure starts at `replicationFactor = 2`.

## 8. Guarantees and failure handling

**Delivery.** At-least-once. A message is delivered until it is acknowledged, dead-lettered or
expired. Duplicates are possible after a visibility timeout and when a caller retries a request
whose answer was lost in a failover.

**Ordering.** Priority and FIFO are strict within a shard. A queue with `shards = 1` has strict
global ordering. With more shards, ordering across shards is best-effort: a priority summary can
be up to 200 ms stale, so a lower-priority message can occasionally be delivered while a
higher-priority one waits in another shard. Low priority can starve under sustained
higher-priority load; there is no aging.

**Durability.** Replication is opt-in. A queue created with default settings has one copy of each
shard, and losing that broker loses the shard's messages. For a queue created with a higher
`replicationFactor`: with `acks=all`, an enqueue that returned success is on every in-sync
replica, at least `minInSyncReplicas` of them. It survives the loss of any brokers as long as one
in-sync replica of its shard survives. Logs are held in memory, so losing every in-sync replica of
a shard at once loses that shard's data. While a shard's data is intact, messages are never
dropped silently: every message ends up acknowledged, dead-lettered or counted as expired. A shard
that restarts empty is counted as a reset.

| Failure | What happens |
|---|---|
| A follower's broker dies | The leader removes it from the ISR after `replicaLagMax`. Writes continue while the ISR is at least `minInSyncReplicas`; otherwise enqueues return 503. |
| A leader's broker dies | Its lease expires, an in-sync replica wins the election and the epoch goes up. In-flight messages keep their deadlines and delivery counts. Callers with unanswered requests retry. |
| A leader is cut off from the others | It cannot commit and stops serving when its lease runs out. A new leader is elected; the old leader's requests are rejected by epoch. |
| A broker restarts | It comes back as a new incarnation with an empty log, catches up as a follower and is added back to the ISR. |
| Every in-sync replica of a shard is lost | The shard goes offline, or elects any live replica with possible loss if `uncleanLeaderElection` is on. |
| The broker of an unreplicated shard dies (`replicationFactor = 1`, the default) | The shard is offline until that broker returns. Its messages are lost: the broker takes over again with an empty log, and the reset is counted in a metric. |
| The coordinator is unavailable | Brokers stop serving when their leases run out. Data stays in memory and service resumes when the coordinator returns. Production would run three or five etcd nodes. |

Deadlines are absolute times stamped by the leader. After a failover the new leader checks them
against its own clock, so clock skew between brokers can make a redelivery slightly early or late.

## 9. Metrics

| ID | Metric | Definition |
|---|---|---|
| M-1 | Oldest message age | Per queue: age in seconds of the oldest message that is neither expired nor in flight. |
| M-2 | Message count | Messages ready to be consumed, broken down by priority level (and by queue name). |
| M-3 | Messages in flight | Per queue: messages dequeued but not yet acknowledged. |
| M-4 | Throughput | Per queue: messages enqueued per second and messages acknowledged per second. |
| M-5 | Failures / DLQ | Per queue: count of messages moved to the dead-letter queue. |

The cluster adds replication metrics: in-sync replica count per shard, under-replicated shards,
leader epoch, log end offset, high watermark, leader elections and shard resets.

## 10. Decisions and trade-offs

### Architecture

| Decision | Choice | Why | Alternative considered |
|---|---|---|---|
| Build or describe the cluster | Build it | Durability and failover become implemented, testable behaviour instead of prose. Cost: several times the code, and strict global ordering is lost once `shards > 1`. | Single-node in-memory service with a written scaling story (all the spec requires) |
| Priority levels | Defined per queue at creation | Each queue fits its own workload, one level gives a plain FIFO queue, and adding a level needs no code change (section 6.2). | A fixed `HIGH` / `MEDIUM` / `LOW` enum |
| Replication model | Leader and in-sync replicas | Clients tune durability against availability (`replicationFactor`, `minInSyncReplicas`, `acks`, `uncleanLeaderElection`). Survives one failure with two replicas. | A Raft group per shard through a library: less code and no external process, but a fixed majority quorum and none of those settings |
| Default replication | None: `replicationFactor = 1` | A queue made with defaults works on any cluster size, including one broker, and replication is a cost the client opts into. Cost: a default queue does not survive the loss of its broker. | Three replicas with two in sync by default: safe out of the box, but queue creation fails on fewer than three brokers |
| Coordinator | etcd | One container. Leases give liveness; compare-and-swap transactions give epochs. | ZooKeeper; a Raft metadata quorum inside the brokers, which means rebuilding the parts of etcd we use |
| Election | In-sync replicas race one compare-and-swap | No extra controller role; the coordinator guarantees a single winner. | A controller broker that picks leaders |
| What is replicated | Every state transition, committed before the caller is answered | An acknowledged message cannot reappear, retry counts survive failover, followers never read clocks. | Replicating enqueues only |
| Log storage | In memory on each replica, behind a `ShardLog` interface | Durability comes from replication first; a disk log can be added behind the same interface. | A file-backed log from the start |
| Concurrency | One lock per shard; wait for commit outside the lock | Each transition moves a message between structures atomically, so a message is always in exactly one place. | Lock-free structures; a single writer thread per shard |
| Ready structure | A sorted map per priority level, keyed by offset | A redelivered message returns to its exact position; expired messages are removed exactly. | Deques (redelivery order is approximate); one heap |
| Cross-shard priority | Summaries exchanged every 200 ms | Honours priority across shards without an extra round trip per dequeue. | Round-robin across shards; asking every shard on each dequeue |
| Client interface | REST; any broker forwards to the leader | Works with plain HTTP clients such as curl, and keeps the harness simple. | A client library that routes to leaders itself; a gRPC stream that pushes messages to consumers, which would start a message's visibility timeout before the consumer looks at it |
| Broker-to-broker transport | gRPC with protobuf | Every committed write waits on replication. A stream per shard over one connection per broker pair removes the poll round trip, and the schema is typed and can evolve. | REST with a JSON long poll per shard; a custom binary protocol, where we would write framing and versioning ourselves |
| Timeouts | Reclaim on every dequeue, plus a sweeper | Exact against the clock for active shards; idle shards still converge. | Sweeper only |

### Questions the spec leaves open

| Question | Decision |
|---|---|
| Priority strictness | Strict within a shard, no aging; best-effort across shards. |
| Retry limit N | At most N+1 deliveries; dead-lettered when the last one times out. |
| Redelivery position | The original FIFO position. |
| TTL expiry | Dropped and counted as expired. An in-flight message can still be acknowledged; it is not redelivered once expired. |
| Late acknowledge | Accepted by ID while the message still exists, even after its timeout. An ack means the work succeeded, so rejecting it would only cause redelivery. |
| Empty queue | Returns immediately by default; `waitTime` turns on long polling. |
| Dead-letter access | Read-only listing. |
| Changing settings | Allowed except `priorityLevels`, `shards` and `replicationFactor`; applies to future deliveries. |
| Throughput | Counters for Prometheus, plus a 60-second rate in the JSON endpoint. |
| Explicit negative acknowledge | Not provided; the visibility timeout is the only failure signal. |

## 11. Testing approach

- **State machine:** deterministic unit tests on a manual clock, covering ordering, visibility, redelivery, dead-lettering, TTL and acknowledge edge cases. No sleeps.
- **Replay:** applying a recorded event sequence to a fresh state machine must reproduce the same state. This is what makes a follower equal to its leader.
- **Concurrency:** many producers and consumers against one shard, asserting that no message is lost, no message is held by two consumers at once, and `enqueued = ready + in flight + acked + dead-lettered + expired`.
- **Replication and failover:** several brokers in one process with an in-memory coordinator and a transport that can drop, delay and partition. Each row of the failure table in section 8 has a test.
- **Coordinator:** one contract test suite run against both the in-memory coordinator and etcd (through Testcontainers).
- **HTTP:** every endpoint and error code.
- **End to end:** the harness runs against the docker-compose cluster, kills a shard leader mid-run and reports lost, duplicated and acknowledged counts.

## 12. Not in the plan

| Item | What it would add |
|---|---|
| Log on disk | Survive the loss of every replica of a shard, and full-cluster restarts |
| Log trimming and snapshots | Bounded memory; today the log grows for the life of the shard |
| Changing a queue's shard count | Scale an existing queue |
| Leader rebalancing and replica reassignment | Even load after failures; replacing a dead broker |
| Idempotent enqueue | Remove duplicates from producer retries |
| Explicit negative acknowledge, visibility extension | Faster retry; long-running work |
| Dead-letter redrive | Move dead letters back to the queue |
| Priority aging | Prevent starvation of low priority |

## 13. Deliverables

1. Working, thread-safe implementation — correctness over completeness.
2. Data model / internal design with sensible concurrency controls.
3. Tests: concurrency tests, edge cases, a clear methodology.
4. This README: key decisions, trade-offs, scalability story, what we would do with more time.
5. Producer / consumer stubs demonstrating enqueue → dequeue → ack under concurrency.
