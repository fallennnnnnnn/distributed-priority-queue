# metrics: numbers for Prometheus

This package publishes the numbers of each shard this broker leads, so that a monitoring system
can collect them from `/actuator/prometheus`.

```
  Broker ── "now leading orders/0" ──▶ QueueMeters.publish(replica)
         ── "no longer leading"   ──▶ QueueMeters.withdraw(replica)

  ShardReplica.metrics()
        │   one reading, reused for 1 second
        ▼
     Snapshot ──▶ gauges and counters ──▶ GET /actuator/prometheus
                  tagged with queue, shard and priority
```

A broker publishes a shard's meters while it leads the shard and removes them when it stops, so
no message is counted twice across the cluster. A new leader has applied the same events, so its
totals carry on from the old leader's.

## What is published

| Prometheus name | Kind | Meaning |
|---|---|---|
| `dpq_messages_ready` | gauge | Messages waiting to be consumed. One series per `priority`. |
| `dpq_messages_in_flight` | gauge | Messages handed out and not yet acknowledged. |
| `dpq_oldest_message_age_seconds` | gauge | Age of the oldest waiting message. |
| `dpq_dead_letters` | gauge | Dead letters held right now. |
| `dpq_messages_enqueued_total` | counter | Messages enqueued. |
| `dpq_messages_acked_total` | counter | Messages acknowledged. |
| `dpq_messages_delivered_total` | counter | Deliveries, including redeliveries. |
| `dpq_messages_redelivered_total` | counter | Deliveries after the first. |
| `dpq_messages_dead_lettered_total` | counter | Messages moved to the dead letters. |
| `dpq_messages_expired_total` | counter | Messages dropped because their TTL passed. |
| `dpq_messages_enqueued_per_second` | gauge | Enqueue rate over the last 60 seconds. |
| `dpq_messages_acked_per_second` | gauge | Acknowledge rate over the last 60 seconds. |

And for replication:

| Prometheus name | Kind | Meaning |
|---|---|---|
| `dpq_in_sync_replicas` | gauge | Replicas that have every committed event. |
| `dpq_under_replicated` | gauge | 1 when the in-sync set is smaller than the replication factor. |
| `dpq_leader_epoch` | gauge | Goes up by one with every new leader. |
| `dpq_log_end_offset` | gauge | The offset the next event will get. |
| `dpq_high_watermark` | gauge | Events below this offset are on every in-sync replica. |
| `dpq_leader_elections_total` | counter | Elections this broker won for the shard. |
| `dpq_shard_resets_total` | counter | Elections won without being in sync, which may have lost messages. |

A gauge is a number that goes up and down. A counter only goes up.

One line of the output looks like this:

```
dpq_messages_ready{priority="HIGH",queue="orders",shard="0"} 3.0
```

## From shards to queues

Numbers are published per shard, by whichever broker leads it. Prometheus adds them up per queue:

```
  sum by (queue) (dpq_messages_ready)                   ready messages in a queue
  max by (queue) (dpq_oldest_message_age_seconds)       the oldest message in a queue
  sum by (queue) (rate(dpq_messages_enqueued_total[1m]))    enqueues per second
  sum by (queue) (dpq_under_replicated)                 shards of a queue that are missing a replica
```

## Why there is a snapshot

One collection reads about fifteen numbers from the same shard. Reading them one by one would
lock the shard fifteen times and could mix numbers from different moments. `QueueMeters` takes
one reading per shard and reuses it for a second, so the numbers in one collection belong
together. The replication gauges read the replica's status directly; it needs no lock.

## The class

`QueueMeters` is the only class. The broker tells it whenever a replica starts or stops leading
its shard, and it registers or removes that replica's meters.

The same queue numbers, already added up over the shards and gathered from every leader, are
also available as JSON from `GET /queues/{queue}/metrics` in the [api](../api/README.md) package.
