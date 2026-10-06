# cluster: one broker, and how it finds the right shard

This package is the broker itself. It takes a request such as "enqueue this into `orders`",
works out which shard should handle it, and passes it to that shard's leader: a replica on this
broker, or another broker over the transport.

## What a broker is made of

```
                                     Broker
  ┌────────────────────────────────────────────────────────────────────────┐
  │  QueueService       the front door for every request                   │
  │    ├── ClusterMetadata   a cached copy of the queue facts              │◀── told about ── Coordinator
  │    │                     from the coordinator                          │    every change
  │    ├── Router            which shard?                                  │
  │    ├── LocalShards       the replicas this broker holds, and their     │
  │    │                     role: leader, or follower with a fetcher      │
  │    └── Peers             the other live brokers, one BrokerApi each    │──▶ BrokerTransport (gRPC)
  │                                                                        │
  │  LocalApi           what the other brokers may ask this one            │◀── gRPC server
  │                                                                        │
  │  on a schedule:  sweeper (reclaim on led shards)        every 100 ms   │
  │                  ISR upkeep and election check          every ≤ 1 s    │
  │                  priority summary exchange              every 200 ms   │
  └────────────────────────────────────────────────────────────────────────┘
```

## Starting up

1. **Register** with the coordinator under a lease. If the key of an earlier incarnation is still
   there, the broker waits for that lease to run out, up to twice the failure-detection timeout.
2. **Watch** the coordinator. Every change report is handled on the broker's own scheduler.
3. **Load the queues**: for each one, read its settings and shard states and let `LocalShards`
   create a replica for every shard assigned to this broker, as leader or follower.
4. **Stand for election** on any held shard whose leader is not a live broker.
5. **Start the schedule** above.

## An enqueue, step by step

```
  QueueService.enqueue("orders", payload, "HIGH", ttl, shardKey, acks)

   1. ClusterMetadata.find("orders")     the queue's settings and shard states     unknown queue: 404
   2. Router.shardForEnqueue(...)        a shard number, say 2
   3. who leads orders/2?                from the cached shard state               no leader: 503
        this broker ──▶ LocalShards.find(orders/2).enqueue(...)
        another one ──▶ Peers.find(leader).enqueue(orders/2, epoch, ...)           over gRPC
   4. a future with the message ID, completed when the event is committed
```

Dequeue, ack, metrics and dead letters follow the same path. Only step 2 differs.

A passed-on request names the epoch this broker believes in. If the broker asked no longer
leads the shard in that epoch it answers `NotLeaderException`; the metadata is read again and
the request is tried once more on the leader it now names. One retry only, so a leader change in
mid-request is covered and a request is never passed on to a third broker.

## How the router picks a shard

```
  enqueue with shardKey "customer-42"   ── hash ──▶  always the same shard
  enqueue without a key                 ── turns ─▶  shard 0, 1, 2, 0, 1, 2 ...
  ack of message "2-17"                 ──────────▶  shard 2, it is written in the ID
  dequeue                               ──────────▶  the shard with the best ready priority,
                                                     then the next shard if that one is empty
```

- **Same key, same shard:** messages that share a `shardKey` stay in order, because order is
  kept within a shard.
- **Dequeue:** for its own shards the router reads `highestReadyRank` directly; for the shards
  other brokers lead it uses the summaries they sent in the last exchange. Shards that tie take
  turns, so none is favoured. A shard whose leader cannot be reached is passed over, so the rest
  of the queue can still be consumed.
- **One shard:** a queue with `shards = 1` has strict priority and FIFO order across the whole
  queue. With more shards the order is strict inside each shard and best-effort across them.

## Creating a queue

```
  QueueService.createQueue(config)
        │  replicationFactor > live brokers: 400
        │  spreads each shard's replicas round-robin over the live brokers;
        │  the first is the leader, the epoch is 0, all of them are in sync
        ▼
  Coordinator.createQueue(...)        settings and all shard states in one step
        │                             name already taken: 409
        │  tells every broker "queue orders changed"
        ▼
  ClusterMetadata.refresh("orders")   reads the new facts
        ▼
  LocalShards.sync(...)               creates a ShardReplica for each shard given to this broker,
                                      as leader, or as follower with a FollowerFetcher
```

A broker never creates replicas directly from a request. It creates them when the coordinator
says so, which keeps every broker's view the same. The same `sync` runs on every later change to
a shard's state, which is how a replica learns that it has become leader or that it must follow
someone new.

## Elections, leases and fencing

- **Election:** on the schedule, and whenever the set of live brokers changes, the broker looks
  at every shard it holds. If the leader is not a live broker and this replica is in the in-sync
  set, it tries one compare-and-swap: leader = me, epoch + 1, in-sync set minus the dead leader.
  Exactly one replica wins; the others see the new leader on their next refresh.
- **Unclean election:** if no in-sync replica is alive and the queue allows it, or the shard has
  only one replica, any live replica may take over, and the replica counts a reset.
- **Lease lost:** when the coordinator stops confirming the lease, the broker is *fenced*: its
  leaders answer 503 and it does not stand for election. It registers again as the same
  incarnation, reads every shard's state, and either carries on leading or steps down to follow
  whoever was elected meanwhile.
- **Fencing on the wire:** `LocalApi` refuses any request whose epoch is not the one this broker
  leads the shard in, with `NotLeaderException`.

## Waiting for a message

`dequeue(queue, waitTime)` on an empty queue does not give up at once. It looks again every 50 ms
until `waitTime` has passed, 20 seconds at most, and then answers "empty". No thread is held
while it waits.

## The classes

| Class | What it is |
|---|---|
| `Broker` | One instance of the service. Registers with the coordinator, holds replicas, stands for elections, runs the scheduled tasks. Its `LocalApi` serves the other brokers. |
| `QueueService` | Everything a client can ask: create, describe, update, enqueue, dequeue, ack, metrics, dead letters. Finds the leader and passes the request on. |
| `Router` | Picks the shard for an enqueue and the shard order for a dequeue. |
| `ClusterMetadata` | This broker's cached copy of queue settings and shard states. Refreshed on every change report. |
| `LocalShards` | The shard replicas this broker holds, their roles, and the fetcher of each follower. |
| `Peers` | The other live brokers and the connection to each. |
| `BrokerApi` | What one broker asks of another: enqueue, dequeue, ack, metrics, dead letters, ready summary, open a fetch stream. Every call names an epoch. |
| `BrokerTransport` | How brokers reach each other: gRPC in a cluster, `NONE` for a single broker. |
| `QueueMetadata` | A queue's settings plus the state of each shard. |
| `QueueMetrics` | One queue's numbers, added up over its shards. |
| `BrokerSettings` | The broker's configuration, read from `dpq.*` properties and checked at startup. |
| `BrokerConfiguration`, `ClusterConfiguration` | Spring wiring: the clock, the coordinator, the transport, the broker and the gRPC server. |

Several brokers can run in one JVM, each with its own settings and coordinator connection,
which is how the multi-broker tests work.
