# Testing approach

570 tests, run with `make test`. The ideas behind them:

- **Time is moved by hand.** Every time-dependent rule reads one injected clock, and tests advance it themselves. Timeout, TTL and lag tests are exact and never sleep to let time pass.
- **Invariants, not interleavings.** Concurrency tests run real threads and then assert what must hold however they interleaved: no message is lost, no message is held by two consumers at once, and `enqueued = ready + in flight + acked + dead-lettered + expired`.
- **Replay equals the original.** Applying a recorded event sequence to a fresh state machine must reproduce the same state. This is the property that makes a follower equal to its leader.
- **Failures are injected, not waited for.** The multi-broker tests run brokers in one JVM over a network and a coordinator connection that the test can hold, partition and cut.
- **One contract, two coordinators.** The same test suite runs against the in-memory coordinator and against etcd.
- **The tests can fail.** At four points I planted a bug and checked that tests caught it: removing the shard lock, letting the high watermark ignore followers, letting an out-of-sync replica be elected, and skipping the state rebuild when a leader steps down.

| What | Test classes | Tests |
|---|---|---|
| Queue behaviour on one shard: ordering, visibility, retries, dead letters, TTL, acks, priority levels, metrics | `ShardReplicaTest`, `QueueConfigTest`, `MessageIdTest`, `RateWindowTest` | 64 |
| Event replay | `ShardStateMachineReplayTest` | 10 |
| Concurrency on one shard | `ShardReplicaConcurrencyTest` | 310, mostly repetitions of one race |
| The log | `InMemoryShardLogTest` | 10 |
| Sharding, routing, the sweeper and waiting dequeues on one broker | `QueueServiceTest` | 21 |
| Every REST endpoint and error code, over real HTTP | `QueueApiTest` | 29 |
| Replication rules between two replicas | `ReplicationProtocolTest` | 30 |
| Replication across brokers | `ReplicationTest`, `ReplicatedConcurrencyTest` | 19 |
| Failover: one test per row of the [failure table](guarantees.md#failure-table) | `FailoverTest` | 14 |
| The cluster over real gRPC, and the wire format | `GrpcClusterTest`, `WireMapperTest` | 12 |
| The coordinator contract, in memory and on etcd | `InMemoryCoordinatorTest`, `EtcdCoordinatorTest` | 27 |
| Meters moving with leadership | `QueueMetersTest` | 3 |
| Settings, the harness, and test support | `BrokerSettingsTest`, `QueueHarnessTest`, others | 21 |

Test support, under `src/test/java/.../testsupport`: `ManualClock`, `TestBrokers` (brokers with
the documented defaults and the sweeper effectively off), `InProcessNetwork` (a broker-to-broker
network that tests can hold, partition and delay), `TestCluster` (several brokers sharing one
in-memory coordinator) and `CoordinatorLink` (a coordinator connection a test can cut).

## Where each requirement is tested

| Requirement | Tested in |
|---|---|
| FR-1, FR-2: create and configure queues | `QueueServiceTest`, `QueueApiTest` |
| FR-3 to FR-5: enqueue, message IDs, TTL | `ShardReplicaTest` (Ttl, PriorityLevels), `QueueApiTest` (Enqueue) |
| FR-6, FR-7: priority and FIFO | `ShardReplicaTest` (Ordering), `QueueServiceTest` (Sharding) |
| FR-8: in-flight messages are invisible | `ShardReplicaTest` (Visibility), `ShardReplicaConcurrencyTest` |
| FR-9, FR-10: acknowledge | `ShardReplicaTest` (Acknowledge), `QueueApiTest` (Acknowledge) |
| FR-11 to FR-13: redelivery, retry count, dead letters | `ShardReplicaTest` (Visibility, RetryAndDeadLetter) |
| FR-14: metrics | `ShardReplicaTest` (Metrics), `QueueApiTest` (MetricsAndDeadLetters), `QueueMetersTest` |
| NFR-1: thread safety | `ShardReplicaConcurrencyTest`, `ReplicatedConcurrencyTest` |
| NFR-2: at-least-once | `ShardReplicaConcurrencyTest`, `QueueHarnessTest`, `FailoverTest` |
| NFR-3: several instances | `ReplicationTest` (AcrossBrokers), `GrpcClusterTest` |
| NFR-4: latency | The harness runs in [what a run shows](operations.md#what-a-run-shows) |
| NFR-5: durability | `ReplicationTest` (Committing, InSyncSet), `FailoverTest` |
| NFR-6: node failure | `FailoverTest`, `GrpcClusterTest`, the harness with `--killCommand` |

The requirement IDs are defined in [requirements.md](requirements.md).
