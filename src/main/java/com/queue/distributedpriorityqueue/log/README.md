# log: the diary of a shard

A shard's log is a list of events. Every event gets the next number, called its offset. Events
are only ever added at the end.

The log is the source of truth. The state in [core](../core/README.md) can always be rebuilt by
reading the log from the start.

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
| High watermark | The offset of the first event that is not yet committed. Everything before it is safe. |
| Truncate | Cut off the end of the log. Committed events can never be cut off. |
| End of an epoch | Where one leader's events stop. Two copies of a log use it to find where they last agree. |

## The classes

| Class | What it is |
|---|---|
| `ShardLog` | The interface: append, read, truncate, high watermark, end of an epoch. |
| `InMemoryShardLog` | A log kept in memory. It is never trimmed, so it grows for the life of the shard. |
| `LogEntry` | One row of the picture: offset, epoch and event. |

`InMemoryShardLog` is thread-safe on its own, so it can be read without taking the shard lock.

## What is used today

With one replica per shard, an event is committed the moment it is appended, so the high
watermark always equals the end offset. Epochs and truncation matter once shards are copied
between brokers, which is the next part of the plan.
