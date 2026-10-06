# Guarantees and failure handling

**Delivery.** At-least-once. A message is delivered until it is acknowledged, dead-lettered or
expired. Duplicates are possible after a visibility timeout, and when a request whose answer was
lost in a failover is tried again, whether by the caller or by the broker's one automatic retry.

**Ordering.** Priority and FIFO are strict within a shard. A queue with `shards = 1` has strict
global ordering. With more shards, ordering across shards is best-effort: a priority summary can
be up to 200 ms stale, so a lower-priority message can occasionally be delivered while a
higher-priority one waits in another shard. Low priority can starve under sustained
higher-priority load; there is no aging.

**Durability.** Replication is opt-in. A queue created with default settings has one copy of each
shard, and losing that broker loses the shard's messages. For a queue created with a higher
`replicationFactor`: with `acks=all`, an enqueue that returned success is on every in-sync
replica, at least `minInSyncReplicas` of them. It survives the loss of any brokers as long as one
in-sync replica of its shard survives. Logs are held in memory, so losing every in-sync replica of
a shard at once loses that shard's data. While a shard's data is intact, messages are never
dropped silently: every message ends up acknowledged, dead-lettered or counted as expired. A shard
that restarts empty is counted as a reset.

## Failure table

Each row has a test in `FailoverTest`.

| Failure | What happens |
|---|---|
| A follower's broker dies | The leader removes it from the ISR after `replicaLagMax`. Writes continue while the ISR is at least `minInSyncReplicas`; otherwise enqueues return 503. |
| A leader's broker dies | Its lease expires, an in-sync replica wins the election and the epoch goes up. In-flight messages keep their deadlines and delivery counts. Callers with unanswered requests retry. |
| A leader is cut off from the others | It cannot commit and stops serving when its lease runs out. A new leader is elected; the old leader's requests are rejected by epoch. |
| A broker restarts | It comes back as a new incarnation with an empty log, catches up as a follower and is added back to the ISR. |
| Every in-sync replica of a shard is lost | The shard goes offline, or elects any live replica with possible loss if `uncleanLeaderElection` is on. |
| The broker of an unreplicated shard dies (`replicationFactor = 1`, the default) | The shard is offline until that broker returns. Its messages are lost: the broker takes over again with an empty log, and the reset is counted in a metric. |
| The coordinator is unavailable | Brokers stop serving when their leases run out. Data stays in memory and service resumes when the coordinator returns. Production would run three or five etcd nodes. |

Deadlines are absolute times stamped by the leader. After a failover the new leader checks them
against its own clock, so clock skew between brokers can make a redelivery slightly early or late.

## Behaviour choices

| Question | Decision |
|---|---|
| Priority strictness | Strict within a shard, no aging; best-effort across shards. |
| Retry limit N | At most N+1 deliveries; dead-lettered when the last one times out. |
| Redelivery position | The original FIFO position. |
| TTL expiry | Dropped and counted as expired. An in-flight message can still be acknowledged; it is not redelivered once expired. |
| Late acknowledge | Accepted by ID while the message still exists, even after its timeout. An ack means the work succeeded, so rejecting it would only cause redelivery. |
| Empty queue | Returns immediately by default; `waitTime` turns on long polling. |
| Dead-letter access | Read-only listing. |
| Changing settings | Allowed except `priorityLevels`, `shards` and `replicationFactor`; applies to future deliveries. |
| Throughput | Counters for Prometheus, plus a 60-second rate in the JSON endpoint. |
| Explicit negative acknowledge | Not provided; the visibility timeout is the only failure signal. |
