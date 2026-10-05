# metrics: numbers for Prometheus

This package publishes each shard's numbers so that a monitoring system can collect them from
`/actuator/prometheus`.

```
  ShardReplica.metrics()
        │   one reading, reused for 1 second
        ▼
     Snapshot ──▶ gauges and counters ──▶ GET /actuator/prometheus
                  tagged with queue, shard and priority
```

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

A gauge is a number that goes up and down. A counter only goes up.

One line of the output looks like this:

```
dpq_messages_ready{priority="HIGH",queue="orders",shard="0"} 3.0
```

## From shards to queues

Numbers are published per shard. Prometheus adds them up per queue:

```
  sum by (queue) (dpq_messages_ready)                   ready messages in a queue
  max by (queue) (dpq_oldest_message_age_seconds)       the oldest message in a queue
  sum by (queue) (rate(dpq_messages_enqueued_total[1m]))    enqueues per second
```

## Why there is a snapshot

One collection reads about fifteen numbers from the same shard. Reading them one by one would
lock the shard fifteen times and could mix numbers from different moments. `QueueMeters` takes
one reading per shard and reuses it for a second, so the numbers in one collection belong
together.

## The class

`QueueMeters` is the only class. The broker tells it about every shard replica it creates, and
it registers the meters for that replica.

The same numbers, already added up per queue, are also available as JSON from
`GET /queues/{queue}/metrics` in the [api](../api/README.md) package.
