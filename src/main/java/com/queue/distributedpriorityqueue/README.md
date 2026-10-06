# Code map

One broker process, from the outside in. Each box is a package, and each package has its own
short README.

```
              HTTP request                         gRPC from another broker
                   │                                        │
          ┌────────▼────────┐                      ┌────────▼────────┐
          │       api       │                      │    transport    │   protobuf in and out
          └────────┬────────┘                      └────────┬────────┘
          ┌────────▼────────────────────────────────────────▼────────┐        ┌────────────────┐
          │                         cluster                          │───────▶│  coordination  │
          │  one broker: finds the shard, or the broker that has it  │        └────────────────┘
          └────────────────────────────┬─────────────────────────────┘    who is alive, which queues
          ┌────────────────────────────▼─────────────────────────────┐    exist, who leads each shard
          │                       replication                        │
          │   one copy of one shard: a lock, a log and the state     │
          └───────────────┬─────────────────────────┬────────────────┘
                    ┌─────▼───┐                 ┌───▼─────┐
                    │   log   │                 │  core   │   log: what happened, in order
                    └─────────┘                 └─────────┘   core: the rules of the queue

 On the side:   metrics (numbers for Prometheus)   exception (errors)   demo (load test)
```

Arrows point from the caller to what it uses. `core` uses nothing else, which is why it is the
easiest place to start.

| Package | In one line |
|---|---|
| [core](core/README.md) | The rule book: what a message is and what can happen to it. |
| [log](log/README.md) | The diary of a shard: every change, numbered, in order. |
| [replication](replication/README.md) | One copy of a shard, as leader or follower. Runs every request under one lock. |
| [coordination](coordination/README.md) | The shared notebook that all brokers agree on: in memory, or in etcd. |
| [cluster](cluster/README.md) | One broker: its lease, its replicas, routing, forwarding and elections. |
| [transport](transport/README.md) | Carries broker-to-broker calls over gRPC. |
| [api](api/README.md) | The HTTP front door. |
| [metrics](metrics/README.md) | Numbers for Prometheus. |
| [exception](exception/README.md) | The errors a caller can get. |
| [demo](demo/README.md) | Producers and consumers that load running brokers. |

A good reading order is the order of this table.

## Words used everywhere

```
 queue "orders"
 ├── shard 0      A queue is split into shards.
 ├── shard 1      Each shard is a small queue of its own,
 └── shard 2      with its own log, its own state and its own lock.

 shard 0          A shard has several replicas, one per broker that holds it.
 ├── broker-1     leader: serves requests and appends to the log
 ├── broker-2     follower: copies the leader's log
 └── broker-3     follower
```

- **Broker:** one running instance of this service.
- **Shard:** one slice of a queue.
- **Replica:** one broker's copy of a shard. One replica leads, the others follow.
- **In-sync replicas (ISR):** the replicas that have every committed event. Only they may become leader.
- **Epoch:** goes up by one each time a shard gets a new leader. Requests from an older epoch are refused.
- **High watermark:** the log position below which every in-sync replica has the events. Those are committed.
- **Incarnation:** a random ID a broker makes when it starts. A restarted broker is a new incarnation with an empty log.

## One broker or many

The same code runs in both cases. With `dpq.coordinator.type=memory` a broker is a cluster of
one: the coordinator lives inside the process and there is no gRPC server. With `etcd` the
broker registers in a shared etcd, starts its gRPC server, and holds the replicas the metadata
assigns to it.
