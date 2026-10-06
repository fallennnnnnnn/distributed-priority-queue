# Requirements

What the assignment asks for, and what this implementation adds. The design that meets these
is in the [README](../README.md).

## Problem summary

Build the **Queue Service**: a priority queue that multiple backend services share to enqueue and
process work items. The service owns queues and the full message lifecycle (enqueue, dequeue,
acknowledge, retry, dead-letter) and exposes operational metrics.

Three things define it:

- **Priority ordering** — consumers get the highest-priority available message, FIFO within a priority.
- **At-least-once delivery** — a dequeued message stays hidden ("in flight") until it is acknowledged;
  if the consumer does not acknowledge in time, the message is redelivered, and after too many
  redeliveries it is moved to a dead-letter queue.
- **Safe under concurrency** — many producers and consumers call the service at the same time.

## Scope

The spec requires an in-memory, single-node service plus a written scaling story. We chose to build
the scaling story instead of only describing it (see [decisions](decisions.md) for why and what it costs).

| In scope | Out of scope |
|---|---|
| The Queue Service and its REST API | Real producer / consumer services (test harnesses are enough) |
| A multi-broker cluster: sharded queues, replicated shards, leader failover | The monitoring system itself (we only expose the data) |
| Tunable settings per queue, per request and per broker | Disk persistence, log trimming, changing a queue's shard count ([decisions](decisions.md)) |
| Tests, including concurrency and failover tests | |
| Producer / consumer harness and a failover demo | |
| docker-compose with three brokers and etcd | |

## Message lifecycle

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

## Functional requirements

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
| FR-14 | Metrics | Expose the queue-level metrics listed in [operations](operations.md#metrics) in a form a system like Prometheus can scrape. |

Added by our design, beyond the spec:

| ID | Area | Requirement |
|---|---|---|
| EX-1 | Priority levels | The client defines the queue's priority levels when creating it. `HIGH` / `MEDIUM` / `LOW` is the default. |
| EX-2 | Sharding | A queue is split into a configurable number of shards so load spreads across brokers. |
| EX-3 | Replication | Each shard is replicated; the client sets the replication factor and the minimum number of in-sync replicas. |
| EX-4 | Failover | When a shard's leader fails, an in-sync replica takes over without losing committed messages. |
| EX-5 | Describe | A client can read each shard's leader, replicas and current in-sync replicas. |
| EX-6 | Tunability | Clients choose their own trade-offs through the [settings](api.md#tunable-settings). |

## Non-functional requirements

| ID | Requirement | Approach |
|---|---|---|
| NFR-1 | **Thread safety (mandatory).** Correct with multiple concurrent producers and consumers. | One lock per shard; concurrency tests assert invariants |
| NFR-2 | **At-least-once delivery.** A message is delivered until it is acknowledged, dead-lettered or expired. | Visibility timeout and redelivery; every transition replicated before the caller is answered |
| NFR-3 | **Horizontal scalability.** The architecture must extend to multiple instances of the service. | Built: shards spread across brokers |
| NFR-4 | **Latency.** p95 < 100 ms for enqueue and dequeue within a region (design target). | One replication round trip per write. Measured p95 is under 10 ms on a healthy three-broker cluster ([measured results](operations.md#what-a-run-shows)) |
| NFR-5 | **Durability.** Messages are not silently lost. | Built: replication to in-sync replicas, chosen per queue; the default is no replication ([guarantees](guarantees.md) states the limits) |
| NFR-6 | **Node failure.** Handle one queue instance going down. | Built: for replicated queues, leader election from the in-sync replicas |

## Deliverables

| Deliverable | Where |
|---|---|
| Working, thread-safe implementation | `src/main/java`; start with `ShardStateMachine`, which holds every queue rule, and `ShardReplica`, which holds the lock and the replication |
| Data model and internal design | [README](../README.md), architecture section |
| Tests | `src/test/java`; [testing](testing.md) |
| Key decisions, trade-offs, scalability story, what we would do with more time | [decisions](decisions.md), [guarantees](guarantees.md) |
| Producer / consumer stubs | `QueueHarness`, run with `make harness` |
