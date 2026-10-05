# replication: one copy of one shard

`ShardReplica` is one broker's copy of a shard. It owns three things:

```
            ShardReplica
  ┌──────────────────────────────┐
  │  lock                        │   one request at a time
  │  log            (log)        │   what happened, in order
  │  state machine  (core)       │   where every message is now
  └──────────────────────────────┘
```

## How a write works

Enqueue, dequeue and ack all follow the same four steps.

```
  caller
    │   enqueue / dequeue / ack
    ▼
  ┌─ take the lock ───────────────────────────────────────┐
  │ 1. decide    the state machine says what should happen │
  │ 2. append    the event goes into the log               │
  │ 3. apply     the state machine changes                 │
  │ 4. commit    the high watermark moves forward          │
  └─ release the lock ────────────────────────────────────┘
    │
    ▼
  the caller's future completes with the answer
```

Steps 1 to 3 happen together under the lock. That is what makes the queue safe with many
producers and consumers: two consumers can never be handed the same message, because the second
one only gets the lock after the first message has already moved to "in flight".

## Why the answer is a future

The caller gets a `CompletableFuture`, and it completes only after the event is committed.

Today a shard has one replica, so an event is committed as soon as it is appended and the future
is already complete when it is returned. When shards are copied to other brokers, step 4 will
wait until the copies have the event. Callers will not have to change.

The future is completed after the lock is released, so no caller code ever runs while the shard
is locked.

## What each operation does

| Operation | Under the lock |
|---|---|
| `enqueue` | Adds the message to its priority lane. Answers with the message ID. |
| `dequeue` | First runs `reclaim`, then hands out the first message of the highest lane. |
| `ack` | Removes the message if it still exists and was delivered. Answers true or false. |
| `reclaim` | Returns timed-out messages to their lanes, dead-letters or expires what is due. |
| `metrics` | Runs `reclaim`, then reads the numbers. |

`dequeue` runs `reclaim` first so that a message whose deadline has just passed is available
again at once, without waiting for the background sweeper.

## A hint for routing

`highestReadyRank()` says which priority level is the best one with a ready message. It is read
without the lock, so it can be a moment out of date. The router in
[cluster](../cluster/README.md) uses it to choose which shard a dequeue tries first.

## Not built yet

Follower replicas, copying events from a leader, and tracking which copies are in sync. Today
every replica is the leader of its shard.
