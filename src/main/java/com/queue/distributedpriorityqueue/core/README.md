# core: the rule book of the queue

Everything about how a queue behaves lives here: what a message is, what can happen to it, and
which message comes out next.

This package knows nothing about HTTP, threads, networks or Spring. It is plain Java that you can
run in a unit test with a clock you move by hand.

## Where a message can be

`ShardStateMachine` holds the messages of one shard. A message is always in exactly one of three
places.

```
   READY: one lane per priority level, oldest message first
  ┌────────┬───────────────┐
  │ HIGH   │ 4  9          │ ◀── dequeue takes the first message
  │ MEDIUM │ 2  5  7       │     of the first lane that is not empty
  │ LOW    │ 1  3          │
  └────────┴───────────────┘
        │ Delivered               ▲ Requeued
        ▼                         │ (deadline missed, retries left)
  ┌────────────────────────┐      │
  │ IN FLIGHT   6  8       │──────┘
  │ hidden until deadline  │──── Acked ───▶ gone for good
  └────────────────────────┘
        │ DeadLettered
        ▼ (deadline missed, no retries left)
  ┌────────────────────────┐
  │ DEAD LETTERS   0       │
  └────────────────────────┘

  Expired (TTL passed) removes a message from READY or IN FLIGHT.
```

The numbers are messages. A message's number is the position of its `Enqueued` event in the
shard's log, and its ID is `{shard}-{number}`, for example `0-4`. Lower numbers are older, so
"oldest first" is simply "lowest number first".

## Decide, then apply

The state machine has two kinds of methods.

```
  request ──▶  decide  ──▶  event  ──▶  apply  ──▶  new state
              reads only                the only place
              the state                 the state changes
```

- **Decide:** `enqueue`, `dequeue`, `ack` and `reclaim` look at the state and answer with the
  event that should happen. They change nothing.
- **Apply:** `apply(offset, event)` changes the state.

Why split it? A second copy of the shard can become identical to the first just by applying the
same events in the same order. It never needs to make a decision of its own, and it never reads
its own clock, because every time value is written into the event.

## The six events

| Event | Meaning |
|---|---|
| `Enqueued` | A new message entered its priority lane. |
| `Delivered` | A consumer took the message. It is hidden until its deadline. |
| `Acked` | The consumer finished the work. The message is gone. |
| `Requeued` | The deadline passed with retries left. The message is back in its old place in line. |
| `DeadLettered` | The deadline passed with no retries left. The message is parked as a dead letter. |
| `Expired` | The message outlived its TTL. It is dropped and counted. |

## The rules in plain words

- **Order:** highest priority first. Within a priority, oldest first.
- **Hidden while in flight:** a delivered message cannot be taken by anyone else until its
  deadline (`visibleAt`).
- **Missed deadline:** the message goes back to its old place in line. If it has already been
  delivered more than `maxRetries` times, it becomes a dead letter instead.
- **Ack:** counts as long as the message still exists and was delivered at least once, even if
  the ack is late. An ack means the work succeeded.
- **TTL:** a message past its TTL is never delivered. If it is in flight when the TTL passes, it
  can still be acked, but it is not redelivered.
- **Reclaim:** `reclaim(now)` is the method that notices missed deadlines and passed TTLs.

## The classes

| Class | What it is |
|---|---|
| `ShardStateMachine` | The messages of one shard and all the rules above. |
| `ShardEvent` | The six events. |
| `QueueConfig` | A queue's settings, checked when created. Priority levels are listed highest first. |
| `MessageId` | `{shard}-{offset}`. An ack can find its shard from the ID alone. |
| `Message` | A message as the state machine holds it. Only the state machine changes it. |
| `MessageView` | A frozen copy of a message, safe to hand to callers. |
| `MessageState` | `READY`, `IN_FLIGHT` or `DEAD`. |
| `ShardMetrics` | The shard's numbers at one moment. |
| `RateWindow` | Counts events per second over the last 60 seconds. |

## One thing to remember

`ShardStateMachine` is not thread-safe. It relies on its owner, `ShardReplica` in the
[replication](../replication/README.md) package, to hold a lock around every call.
