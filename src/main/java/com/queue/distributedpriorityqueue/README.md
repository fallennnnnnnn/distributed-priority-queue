# Code map

One broker process, from the outside in. Each box is a package, and each package has its own
short README.

```
              HTTP request
                   │
          ┌────────▼────────┐
          │       api       │   JSON in, JSON out, errors as HTTP codes
          └────────┬────────┘
          ┌────────▼────────┐        ┌────────────────┐
          │     cluster     │───────▶│  coordination  │   who is alive, which queues exist,
          │ finds the shard │        └────────────────┘   who leads each shard
          └────────┬────────┘
          ┌────────▼────────┐
          │   replication   │   one copy of one shard: a lock, a log and the state
          └───┬─────────┬───┘
        ┌─────▼───┐ ┌───▼─────┐
        │   log   │ │  core   │   log: what happened, in order
        └─────────┘ └─────────┘   core: the rules of the queue

 On the side:   metrics (numbers for Prometheus)   exception (errors)   demo (load test)
```

Arrows point from the caller to what it uses. `core` uses nothing else, which is why it is the
easiest place to start.

| Package | In one line |
|---|---|
| [core](core/README.md) | The rule book: what a message is and what can happen to it. |
| [log](log/README.md) | The diary of a shard: every change, numbered, in order. |
| [replication](replication/README.md) | One copy of a shard. Runs every request under one lock. |
| [coordination](coordination/README.md) | The shared notebook that all brokers agree on. |
| [cluster](cluster/README.md) | One broker: finds the right shard for each request. |
| [api](api/README.md) | The HTTP front door. |
| [metrics](metrics/README.md) | Numbers for Prometheus. |
| [exception](exception/README.md) | The errors a caller can get. |
| [demo](demo/README.md) | Producers and consumers that load a running broker. |

A good reading order is the order of this table.

## Three words used everywhere

```
 queue "orders"
 ├── shard 0      A queue is split into shards.
 ├── shard 1      Each shard is a small queue of its own,
 └── shard 2      with its own log, its own state and its own lock.
```

- **Broker:** one running instance of this service.
- **Shard:** one slice of a queue.
- **Replica:** one broker's copy of a shard.

## What is built today

One broker holds one replica of every shard. Brokers copying shards between each other, and
taking over when one dies, is the next part of the plan in [WORK_ITEMS.md](../../../../../../WORK_ITEMS.md).
