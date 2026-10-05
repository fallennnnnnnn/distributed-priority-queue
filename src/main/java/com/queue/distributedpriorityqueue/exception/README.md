# exception: the errors a caller can get

The service reports a problem by throwing one of these. The [api](../api/README.md) package
turns each into an HTTP status.

```
  QueueService throws ──▶ ApiExceptionHandler ──▶ HTTP status + JSON error body
```

| Exception | When | HTTP status |
|---|---|---|
| `QueueNotFoundException` | No queue has that name. | 404 |
| `MessageNotFoundException` | The message cannot be acknowledged: unknown, never delivered, or already gone. | 404 |
| `QueueAlreadyExistsException` | A queue with that name already exists. | 409 |
| `ShardUnavailableException` | The shard has no leader this broker can reach. The caller may retry. | 503 |

A bad value in a request, such as an unknown priority level or a negative TTL, is reported with
Java's own `IllegalArgumentException` and becomes a 400.
