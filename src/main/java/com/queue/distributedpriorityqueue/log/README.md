# log: the diary of a shard

A shard's log is a list of events. Every event gets the next number, called its offset. Events
are only ever added at the end, except when a follower cuts off entries that were never
committed.

The log is the source of truth. The state in [core](../core/README.md) can always be rebuilt by
reading the log from the start, which is what a leader does when it steps down.

## What a log looks like

```
 offset    0     1     2     3     4     5     6
        ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┐
 event  │ enq │ enq │ dlv │ ack │ enq │ dlv │ enq │
 epoch  │  0  │  0  │  0  │  0  │  1  │  1  │  1  │
        └─────┴─────┴─────┴─────┴─────┴─────┴─────┘
                                 ▲                 ▲
                          high watermark = 4   end offset = 7

         committed: kept forever   │   not committed yet: can still be cut off
```

`enq`, `dlv` and `ack` are short for the `Enqueued`, `Delivered` and `Acked` events.

## The words

| Word | Meaning |
|---|---|
| Offset | An event's position in the log. The first event is 0. |
| End offset | The offset the next event will get. |
| Epoch | Which leader wrote the event. It goes up each time a shard gets a new leader. |
| High watermark | The offset of the first event that is not yet committed. Everything before it is on every in-sync replica. |
| Truncate | Cut off the end of the log. Committed events can never be cut off. |
| End of an epoch | Where one leader's events stop. Two copies of a log use it to find where they last agree. |

## How the cluster uses it

- **On a leader**, the gap between the high watermark and the end offset is what is waiting for
  followers. With one replica there is no gap: an event is committed the moment it is appended.
- **On a follower**, the log fills from the leader's fetch stream, and the high watermark is
  whatever the leader last reported. The follower applies only entries below it.
- **After a leader change**, a follower may hold entries from the old epoch that were never
  committed. The new leader tells it where that epoch ends in its own log, and the follower
  truncates to the lower of the two ends, which is where the logs last agree.

## The classes

| Class | What it is |
|---|---|
| `ShardLog` | The interface: append, read from an offset, truncate, high watermark, end of an epoch. |
| `InMemoryShardLog` | A log kept in memory. It is never trimmed, so it grows for the life of the shard. |
| `LogEntry` | One row of the picture: offset, epoch and event. |

`InMemoryShardLog` is thread-safe on its own, so it can be read without taking the shard lock.
A log on disk would go behind the same interface; durability today comes from replication.
