# Distributed Priority Queue Service

A priority queue service with at-least-once delivery, retries and dead-lettering. It runs as a
single broker or as a cluster: queues are split into
shards, each shard is replicated between brokers, and an in-sync replica takes over when a
leader fails.

Everything described here is built and tested. The [requirements](docs/requirements.md) it meets,
the [API and settings](docs/api.md), the [guarantees](docs/guarantees.md), the
[decisions](docs/decisions.md) behind it, the [testing approach](docs/testing.md) and the
[operations guide](docs/operations.md) with measured results each have their own page.
[WORK_ITEMS.md](WORK_ITEMS.md) is the plan it was built from.

## Running it

Needs JDK 17; the cluster also needs Docker. `make help` lists every target.

| Command | What it does |
|---|---|
| `make run` | One broker on port 8080, no other process |
| `make harness` | Producers and consumers against it; checks that no message was lost |
| `make cluster` | etcd and three brokers on ports 8081, 8082 and 8083 |
| `make cluster-harness` | The harness on a queue replicated three ways, killing a leader half way |
| `make create-queue`, `make enqueue`, `make dequeue`, `make ack ID=0-0`, `make metrics` | The REST API by hand |
| `make test` | The tests |

## Architecture

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

| Term | Meaning |
|---|---|
| Broker | One instance of the service. |
| Shard | One slice of a queue, with its own log, state and lock. |
| Replica | A broker's copy of a shard. One replica is the leader, the rest are followers. |
| In-sync replicas (ISR) | The replicas that have every committed event. Only they can become leader. |
| Epoch | A counter that goes up each time a shard gets a new leader. Used to reject stale leaders. |
| High watermark | The log position up to which every in-sync replica has the events. Events before it are committed. |
| Incarnation | An ID a broker generates when its process starts. A restarted broker is a new incarnation. |

Inside a broker, a request passes through four layers:

```
REST API ──▶ Router ──▶ Shard replica (leader) ──▶ log ──▶ state machine
               └─ forwards to another broker when that broker leads the shard
```

### Priority levels

The client defines a queue's priority levels when creating it: an ordered list of 1 to 10 names,
highest first. The default is `HIGH, MEDIUM, LOW`. An enqueue with no priority gets the lowest
level and an unknown level is rejected. A level is data, not code: each one is a lane in the
shard, so a queue can have two levels or five, one level is a plain FIFO queue, and metrics are
labelled with the queue's own names.

### Shard state machine

All queue behaviour lives in one class, `ShardStateMachine`. Its state is one sorted lane per
priority level keyed by log offset, the in-flight messages indexed by visibility deadline, a TTL
index, the dead letters and counters. State changes only by applying events from the shard's log:
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

`reclaim` runs at the start of every dequeue and on a background sweeper. The leader stamps every
time value into the event, so followers apply events without reading their own clocks.

### Sharding

- **Enqueue routing:** an optional `shardKey` hashes to a shard, so messages with the same key keep their order. Without a key, enqueues go round-robin.
- **Message ID:** `{shard}-{offset}`. An acknowledge routes to the right shard without a lookup.
- **Dequeue routing:** every 200 ms each broker asks the others for the highest ready priority of the shards they lead. A dequeue goes to the shard with the highest ready priority and falls through to the next shard if that one is empty.
- **Any broker:** a client can call any broker; the broker forwards to the shard leader.

### Replication

Each shard is a log of events. The leader appends. Each follower keeps a gRPC stream open to the
leader: it states its log position, the leader sends new events as they are appended, and the
follower reports how far it has got.

```
offset          0    1    2    3    4    5    6    7
Leader    B1   enq  enq  dlv  ack  enq  dlv  enq  ack
Follower  B2   enq  enq  dlv  ack  enq  dlv  enq
Follower  B3   enq  enq  dlv  ack  enq  dlv
                                             ▲
                         high watermark = 6: offsets 0–5 are on every
                         in-sync replica and are committed
```

- **Commit rule:** the leader answers a caller only when that caller's event is below the high watermark. This holds for enqueue, dequeue and acknowledge, so an acknowledged message cannot reappear after a failover.
- **Locking:** the leader decides, appends and applies under the shard lock, then waits for the commit outside the lock.
- **Followers** apply events only up to the high watermark, so their state never includes an event that could still be discarded.
- **ISR upkeep:** the leader removes a follower that has not caught up within `replicaLagMax` and adds it back once it has. Each change is written to the coordinator first; while it is pending, an event counts as committed only when both the old and the proposed set have it.
- **Minimum ISR:** when the ISR is smaller than `minInSyncReplicas`, enqueues with `acks=all` are rejected with 503. Dequeue and acknowledge keep working so consumers can drain the queue.
- **`acks=leader`** returns once the leader has appended: faster, and lost if the leader fails right after.

### Coordinator, election and fencing

```
/dpq/brokers/{id}                -> address, incarnation   (tied to a lease; vanishes when the broker dies)
/dpq/queues/{name}/config        -> queue settings
/dpq/queues/{name}/shards/{n}    -> { leader, epoch, isr, replicas }
```

- **Liveness:** each broker keeps a lease alive. A broker that has not managed to renew for two thirds of the failure-detection timeout treats its lease as lost and stops leading, before the others can elect a replacement.
- **Election:** when a shard has no live leader, each in-sync replica tries one compare-and-swap on the shard state: leader = me, epoch + 1, ISR minus the dead leader. The coordinator lets exactly one succeed. There is no controller broker.
- **Eligibility:** ISR members are recorded by broker incarnation. A restarted broker has an empty log and a new incarnation, so it cannot be elected until it has caught up and been re-added.
- **Fencing:** every broker-to-broker request names the epoch the sender believes in, and a broker serves it only if it leads the shard in that epoch.
- **Stepping down:** a replaced leader ends its followers' streams, fails the callers still waiting for a commit, and rebuilds its state from the committed part of its log.
- **After a leader change:** log entries carry the epoch that wrote them. A follower whose log diverges from the new leader's truncates to where the two logs last agree, which never removes a committed entry.
- **No in-sync replica alive:** the shard goes offline, unless `uncleanLeaderElection` lets any live replica take over with possible loss. An unreplicated shard (`replicationFactor = 1`, the default) always recovers this way, and the reset is counted.

## Components

Each package has its own README with pictures; the
[code map](src/main/java/com/queue/distributedpriorityqueue/README.md) is the place to start.

| Package | Role | Key classes |
|---|---|---|
| [core](src/main/java/com/queue/distributedpriorityqueue/core/README.md) | The rules of the queue. No Spring, no threads, no I/O. | `ShardStateMachine`, `ShardEvent`, `QueueConfig`, `MessageId` |
| [log](src/main/java/com/queue/distributedpriorityqueue/log/README.md) | The per-shard event log: offsets, epochs, high watermark, truncation. | `ShardLog`, `InMemoryShardLog`, `LogEntry` |
| [replication](src/main/java/com/queue/distributedpriorityqueue/replication/README.md) | One replica of a shard: the lock, the write path, leader and follower roles, the fetch stream, ISR tracking. | `ShardReplica`, `FollowerFetcher`, `FetchRequest`, `FetchListener`, `FetchStream` |
| [coordination](src/main/java/com/queue/distributedpriorityqueue/coordination/README.md) | The shared metadata: live brokers, queues, and each shard's leader, epoch and ISR, with compare-and-swap and watches. | `Coordinator`, `InMemoryCoordinator`, `EtcdCoordinator`, `ShardState` |
| [cluster](src/main/java/com/queue/distributedpriorityqueue/cluster/README.md) | One broker: registration and lease, local replicas and their roles, routing and forwarding, elections, the sweeper. | `Broker`, `QueueService`, `Router`, `ClusterMetadata`, `LocalShards`, `Peers`, `BrokerApi` |
| [transport](src/main/java/com/queue/distributedpriorityqueue/transport/README.md) | The gRPC server and client that carry `BrokerApi` between processes. | `GrpcBrokerServer`, `GrpcBrokerTransport`, `broker.proto` |
| [api](src/main/java/com/queue/distributedpriorityqueue/api/README.md) | The REST API: JSON in and out, exceptions to HTTP statuses. | `QueueController`, `ApiModel`, `ApiExceptionHandler` |
| [metrics](src/main/java/com/queue/distributedpriorityqueue/metrics/README.md) | Prometheus meters for the shards this broker leads. | `QueueMeters` |
| [exception](src/main/java/com/queue/distributedpriorityqueue/exception/README.md) | The errors a caller can get. | `NotLeaderException`, `NotEnoughReplicasException`, ... |
| [demo](src/main/java/com/queue/distributedpriorityqueue/demo/README.md) | Producers and consumers that load running brokers over REST and check that nothing was lost. | `QueueHarness` |

Deployment files: [Dockerfile](Dockerfile), [docker-compose.yml](docker-compose.yml) (etcd, three
brokers, optional Prometheus) and [deploy/prometheus.yml](deploy/prometheus.yml).
