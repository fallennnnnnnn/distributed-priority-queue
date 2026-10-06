# exception: the errors a caller can get

The service reports a problem by throwing one of these. The [api](../api/README.md) package
turns each into an HTTP status, and the [transport](../transport/README.md) package carries
them between brokers unchanged.

```
  QueueService throws ──▶ ApiExceptionHandler ──▶ HTTP status + JSON error body
```

| Exception | When | HTTP status |
|---|---|---|
| `QueueNotFoundException` | No queue has that name. | 404 |
| `MessageNotFoundException` | The message cannot be acknowledged: unknown, never delivered, or already gone. | 404 |
| `QueueAlreadyExistsException` | A queue with that name already exists. | 409 |
| `ShardUnavailableException` | The shard has no leader, or its leader cannot be reached from this broker. | 503 |
| `NotLeaderException` | The broker asked does not lead the shard in the epoch the caller named. Inside the cluster this triggers one retry on the current leader; a client only sees it if that retry fails too. | 503 |
| `NotEnoughReplicasException` | An `acks=all` enqueue while the in-sync set is smaller than `minInSyncReplicas`. | 503 |
| `CommitTimeoutException` | The in-sync followers did not confirm a write within `commitTimeout`. The event stays in the leader's log and may still be committed, so a retry can cause a duplicate. | 503 |
| `BrokerUnreachableException` | A request passed on to another broker failed before that broker answered. | 503 |
| `CoordinatorUnavailableException` | The coordinator did not answer, for example while creating a queue. | 503 |

Every 503 means "the request was sound, but the cluster cannot serve it right now"; the caller
may retry. A bad value in a request, such as an unknown priority level or a negative TTL, is
reported with Java's own `IllegalArgumentException` and becomes a 400.
