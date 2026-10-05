# coordination: the shared notebook

When several brokers run together, they must agree on a few facts: who is alive, which queues
exist, and who is in charge of each shard. The coordinator is the one place where those facts
are written down. Every broker reads the same notebook.

## What is written in it

```
  Coordinator
  ├── brokers
  │     ├── broker-1    alive, reachable at host:port
  │     └── broker-2    alive, reachable at host:port
  ├── queues
  │     └── orders      the queue's settings                              version 12
  └── shards
        ├── orders/0    leader: broker-1   epoch: 3   in sync: 1, 2       version 14
        └── orders/1    leader: broker-2   epoch: 1   in sync: 2          version 13
```

| Word | Meaning |
|---|---|
| Leader | The broker that serves the shard right now. |
| Epoch | A counter that goes up by one each time the shard gets a new leader. |
| In sync (ISR) | The replicas that have every committed event. Only they may become leader. |
| Version | A number that changes every time an entry is rewritten. |

## Changing an entry: "only if nobody changed it first"

Every update quotes the version the caller last read. If the entry has changed since, the update
is refused. This is called compare-and-swap.

```
  Broker A reads orders/0 at version 14        Broker B reads orders/0 at version 14

  A: "replace version 14 with ..."   ──▶   accepted, the entry is now version 15
  B: "replace version 14 with ..."   ──▶   refused, the entry is no longer version 14
```

When two brokers try to become leader at the same moment, exactly one wins. The other sees the
refusal, reads the entry again and finds the new leader.

## Hearing about changes

A broker does not ask the coordinator on every request. It subscribes once with `watch`, and the
coordinator tells it what changed: the brokers, a queue or a shard. The broker then reads the new
value. Changes are reported one at a time, in order.

## A broker's identity

```
  ReplicaId = broker ID + incarnation

  broker-1, incarnation a1f3...    the process that started this morning
  broker-1, incarnation 9c2e...    the same broker after a restart
```

The incarnation is a fresh random ID each time a broker process starts. A restarted broker has
an empty log, so it must not be mistaken for the replica it was before. The different
incarnation is how the rest of the cluster can tell.

## The classes

| Class | What it is |
|---|---|
| `Coordinator` | The interface: register a broker, create and update queues, update a shard, watch for changes. |
| `InMemoryCoordinator` | A coordinator inside the JVM. Used by the single-broker setup and by tests that run several brokers in one process. |
| `BrokerInfo` | A live broker: its ID, incarnation, host and ports. |
| `ReplicaId` | A broker ID plus its incarnation. |
| `ShardId` | A queue name plus a shard number, written `orders/0`. |
| `ShardState` | A shard's leader, epoch, in-sync replicas and assigned brokers. |
| `Versioned` | A value together with the version to quote when replacing it. |

## Not built yet

A coordinator backed by etcd, so that brokers in separate processes share one notebook. Leader
elections are also still to come; compare-and-swap is the building block they will use.
