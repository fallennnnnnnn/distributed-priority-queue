# cluster: one broker, and how it finds the right shard

This package is the broker itself. It takes a request such as "enqueue this into `orders`",
works out which shard should handle it, and passes it to that shard's replica.

## What a broker is made of

```
                               Broker
  ┌──────────────────────────────────────────────────────────┐
  │  QueueService       the front door for every request     │
  │    ├── ClusterMetadata   a cached copy of the queue      │◀── told about ── Coordinator
  │    │                     facts from the coordinator      │    every change
  │    ├── Router            which shard?                    │
  │    └── LocalShards       the replicas this broker holds  │
  │            │                                             │
  │            ▼                                             │
  │    ShardReplica   ShardReplica   ShardReplica            │
  │                                                          │
  │  sweeper: runs reclaim on every replica every 100 ms     │
  └──────────────────────────────────────────────────────────┘
```

## An enqueue, step by step

```
  QueueService.enqueue("orders", payload, "HIGH", ttl, shardKey)

   1. ClusterMetadata.find("orders")     the queue's settings          unknown queue: 404
   2. Router.shardForEnqueue(...)        a shard number, say 2
   3. LocalShards.find(orders/2)         the replica of that shard     not on this broker: 503
   4. ShardReplica.enqueue(...)          a future with the message ID
```

Dequeue and ack follow the same path. Only step 2 differs.

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
- **Dequeue:** the router asks each replica for its `highestReadyRank` and tries the best shard
  first. Shards that tie take turns, so none is favoured.
- **One shard:** a queue with `shards = 1` has strict priority and FIFO order across the whole
  queue. With more shards the order is strict inside each shard and best-effort across them.

## Creating a queue

```
  QueueService.createQueue(config)
        │  chooses the brokers for every shard
        ▼
  Coordinator.createQueue(...)        settings and all shard states in one step
        │                             name already taken: 409
        │  tells every broker "queue orders changed"
        ▼
  ClusterMetadata.refresh("orders")   reads the new facts
        ▼
  LocalShards.sync(...)               creates a ShardReplica for each shard given to this broker
```

A broker never creates replicas directly from a request. It creates them when the coordinator
says so, which keeps every broker's view the same.

## Waiting for a message

`dequeue(queue, waitTime)` on an empty queue does not give up at once. It looks again every 50 ms
until `waitTime` has passed, 20 seconds at most, and then answers "empty".

## The classes

| Class | What it is |
|---|---|
| `Broker` | One instance of the service. Registers with the coordinator, holds replicas, runs the sweeper. |
| `QueueService` | Everything a client can ask: create, describe, update, enqueue, dequeue, ack, metrics, dead letters. |
| `Router` | Picks the shard for an enqueue and the shard order for a dequeue. |
| `ClusterMetadata` | This broker's cached copy of queue settings and shard states. |
| `LocalShards` | The shard replicas this broker holds. |
| `QueueMetadata` | A queue's settings plus the state of each shard. |
| `QueueMetrics` | One queue's numbers, added up over its shards. |
| `BrokerSettings` | The broker's configuration, read from `dpq.*` properties and checked at startup. |
| `BrokerConfiguration`, `ClusterConfiguration` | Spring wiring: the clock, the coordinator and the broker. |

## Not built yet

Sending a request to another broker. Today one broker holds every shard, so step 3 always finds
the replica locally. With several brokers, a request for a shard held elsewhere will be passed on
to the broker that leads it.
