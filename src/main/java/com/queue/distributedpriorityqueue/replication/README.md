# replication: one copy of one shard

`ShardReplica` is one broker's copy of a shard. It owns three things, and plays one of two roles:

```
            ShardReplica
  ┌──────────────────────────────┐
  │  lock                        │   one request at a time
  │  log            (log)        │   what happened, in order
  │  state machine  (core)       │   where every message is now
  └──────────────────────────────┘
      as LEADER     serves requests, appends, sends the log to followers, tracks who is in sync
      as FOLLOWER   appends what the leader sends, applies only what is committed
```

## How a write works on the leader

Enqueue, dequeue and ack all follow the same steps.

```
  caller
    │   enqueue / dequeue / ack
    ▼
  ┌─ take the lock ──────────────────────────────────────────────────┐
  │ 1. decide    the state machine says what should happen           │
  │ 2. append    the event goes into the log                         │
  │ 3. apply     the state machine changes                           │
  │ 4. register  a future waiting for the high watermark to pass it  │
  │ 5. send      the new entries go to every follower's stream       │
  └─ release the lock ───────────────────────────────────────────────┘
    │
    │   followers report their log end ──▶ the high watermark moves
    ▼
  the caller's future completes with the answer
```

Steps 1 to 3 happen together under the lock. That is what makes the queue safe with many
producers and consumers: two consumers can never be handed the same message, because the second
one only gets the lock after the first message has already moved to "in flight".

The future completes only after the event is **committed**: below the high watermark, which is
the lowest log end among the in-sync replicas. With one replica that is immediate. With
followers it is one round trip. The future is completed after the lock is released, so no caller
code ever runs while the shard is locked. If the followers do not confirm within
`commitTimeout`, the future fails with `CommitTimeoutException`.

An enqueue with `acks=leader` skips the wait and answers after step 3.

## The fetch stream

```
  FOLLOWER                                              LEADER
  FollowerFetcher ── open(FetchRequest) ─────────────▶  openFetch(): checks role, epoch and that
      │               shard, who I am, my epoch,        the logs agree; refuses with NOT_LEADER,
      │               my log end, my last entry's epoch FENCED_EPOCH or DIVERGED
      │
      │ ◀── onEntries(entries, high watermark) ───────  one batch of up to 500 entries at a time
      │     appendFetched(): append, apply up to HW
      ├──── reportLogEnd(n) ──────────────────────────▶ followerReported(): the next batch, and
      │                                                 maybe a higher high watermark
      │ ◀── onEntries([], high watermark) ────────────  nothing new, but more is now committed
      │
      │ ◀── onEnd(reason) ────────────────────────────  the leader stepped down, was replaced,
      ▼                                                 or the logs diverge
  reconnect from wherever the log now ends; on DIVERGED, truncate first
```

- **One batch in flight:** the leader sends a follower the next batch only when the follower
  has reported the last one, so a slow follower never has more than one batch in transit.
- **Followers apply only committed entries**, so their state never includes an event that could
  still be discarded, and they never need to rebuild it.
- **Diverged logs:** after a leader change a follower may hold entries the old leader never
  committed. The new leader answers `DIVERGED` with where the follower's last epoch ends in its
  own log, and the follower truncates to where the two logs last agree. Committed entries are
  never cut off.

## Keeping the in-sync set right

The leader records when each follower last had everything. A follower that falls behind for
longer than `replicaLagMax` is proposed out of the in-sync set; a follower outside the set that
has caught up is proposed back in. A proposal is a compare-and-swap on the coordinator, made by
the broker. Until the coordinator accepts it, an event is committed only when both the old and
the proposed set have it, because a replica the coordinator has just accepted can be elected at
once. The new set is used only once the write succeeds; a refused proposal is dropped and the
shard's state is read again.

With `acks=all`, an enqueue is refused with `NotEnoughReplicasException` while the in-sync set
is smaller than `minInSyncReplicas`, including one that was still waiting when the set shrank.
Dequeue and ack keep working so consumers can drain the queue.

## Changing role

- **`lead(state)`:** become the leader in the epoch the coordinator recorded, with the in-sync
  set it recorded. Any entries applied but not committed are rolled back by rebuilding the state
  machine from the committed prefix of the log, and the followers' progress is tracked afresh.
- **`follow(state)`:** stop serving. Futures still waiting for a commit fail with
  `NotLeaderException`, every follower's stream is ended, and the state machine is rebuilt from
  the committed prefix, because a leader applies events before they are committed.
- **`wonElection(reset)`:** counts an election won, and whether it was won without being in sync.

## What each operation does

| Operation | Under the lock |
|---|---|
| `enqueue` | Adds the message to its priority lane. Answers with the message ID. |
| `dequeue` | First runs `reclaim`, then hands out the first message of the highest lane. |
| `ack` | Removes the message if it still exists and was delivered. Answers true or false. |
| `reclaim` | Returns timed-out messages to their lanes, dead-letters or expires what is due. |
| `metrics` | Runs `reclaim`, then reads the numbers. |
| `openFetch` | Starts a follower's session and sends its first batch. |
| `appendFetched` | On a follower: appends the leader's entries and applies up to the high watermark. |

`dequeue` runs `reclaim` first so that a message whose deadline has just passed is available
again at once, without waiting for the background sweeper.

## A hint for routing

`highestReadyRank()` says which priority level is the best one with a ready message. It is read
without the lock, so it can be a moment out of date. The router in
[cluster](../cluster/README.md) uses it to choose which shard a dequeue tries first, and brokers
exchange it for the shards they lead.

## The classes

| Class | What it is |
|---|---|
| `ShardReplica` | One copy of a shard: the lock, the write path, both roles, follower sessions and in-sync tracking. `standalone()` makes one that leads itself, for an unreplicated queue. |
| `FollowerFetcher` | Keeps a follower's fetch stream to the leader open, and reopens it from wherever the log now ends whenever it ends. |
| `FetchRequest` | What a follower says when it opens a stream: shard, who it is, epoch, log end, epoch of its last entry. |
| `FetchListener` | The leader's side as the follower sees it: `onEntries`, `onEnd`. Called under the leader's lock, so it must hand off and return. |
| `FetchStream` | The follower's side as the leader sees it: `reportLogEnd`, `close`. |
| `FetchEnd` | Why a stream ended: `NOT_LEADER`, `FENCED_EPOCH`, `DIVERGED` (with where to truncate to), `CLOSED`. |
| `Acks` | `ALL` waits for the commit; `LEADER` answers once the leader has appended. |

Nothing here knows about gRPC. The [transport](../transport/README.md) package carries these
interfaces between processes; the multi-broker tests carry them over an in-process network.
