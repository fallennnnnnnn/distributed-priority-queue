# Decisions and trade-offs

| Decision | Choice | Why | Alternative considered |
|---|---|---|---|
| Build or describe the cluster | Build it | Durability and failover become implemented, testable behaviour instead of prose. Cost: several times the code, and strict global ordering is lost once `shards > 1`. | Single-node in-memory service with a written scaling story (all the spec requires) |
| Priority levels | Defined per queue at creation | Each queue fits its own workload, one level gives a plain FIFO queue, and adding a level needs no code change. | A fixed `HIGH` / `MEDIUM` / `LOW` enum |
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

## What we would do with more time

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
| Security | TLS and authentication on both the REST API and the broker-to-broker port; today both are open |
| Limits | A cap on payload size and on queue depth, with back-pressure to producers |

The first two matter most. Without a log on disk, replication is the only thing between a
message and its loss; without trimming, a long-lived shard eventually runs out of memory.
