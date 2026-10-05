# api: the HTTP front door

This package turns HTTP requests into calls on `QueueService` and turns the answers back into
JSON. It contains no queue logic.

```
  client ── HTTP + JSON ──▶ QueueController ──▶ QueueService   (cluster package)
                                  │
                 something went wrong
                                  ▼
                         ApiExceptionHandler ──▶ HTTP status + a JSON error body
```

## The endpoints

| What | Request | Success | Errors |
|---|---|---|---|
| Create a queue | `POST /queues` | 201 | 400 bad settings, 409 name taken |
| Describe a queue | `GET /queues/{queue}` | 200 | 404 |
| Change settings | `PATCH /queues/{queue}` | 200 | 400, 404 |
| Enqueue | `POST /queues/{queue}/messages` | 201 with the message ID | 400, 404, 503 |
| Dequeue | `POST /queues/{queue}/messages/dequeue` | 200 with a message, or 204 if empty | 404, 503 |
| Acknowledge | `POST /queues/{queue}/messages/{id}/ack` | 204 | 404 nothing to acknowledge |
| Metrics | `GET /queues/{queue}/metrics` | 200 | 404 |
| Dead letters | `GET /queues/{queue}/dlq` | 200 | 404 |

All durations in the JSON are in milliseconds.

## A message's journey over HTTP

Create a queue. Only `name` is required; everything else takes the broker's default.

```bash
curl -X POST localhost:8080/queues -H 'Content-Type: application/json' -d '{"name":"orders","shards":2}'
```

Enqueue. `priority`, `ttlMs` (or `expiresAt`) and `shardKey` are optional.

```bash
curl -X POST localhost:8080/queues/orders/messages -H 'Content-Type: application/json' -d '{"payload":"hello","priority":"HIGH"}'
```

```json
{"messageId":"0-0"}
```

Dequeue. Add `?waitTimeMs=5000` to wait up to five seconds for a message.

```bash
curl -X POST localhost:8080/queues/orders/messages/dequeue
```

```json
{"messageId":"0-0","payload":"hello","priority":"HIGH","deliveryCount":1,
 "enqueuedAt":"2026-10-04T18:06:49.390118Z","visibleUntil":"2026-10-04T18:07:19.406650Z"}
```

Acknowledge before `visibleUntil`, or the message is handed out again.

```bash
curl -X POST localhost:8080/queues/orders/messages/0-0/ack
```

## Errors

Every error has the same JSON shape:

```json
{"detail":"no queue named nope","instance":"/queues/nope","status":404,"title":"Not Found"}
```

| Thrown by the service | HTTP status |
|---|---|
| `IllegalArgumentException` (a bad value) | 400 |
| `QueueNotFoundException`, `MessageNotFoundException` | 404 |
| `QueueAlreadyExistsException` | 409 |
| `ShardUnavailableException` | 503, the caller may retry |

Acknowledging the same message twice gives 204 the first time and 404 the second. The second
call is harmless.

## The classes

| Class | What it is |
|---|---|
| `QueueController` | One method per endpoint. |
| `ApiModel` | The JSON request and response bodies. |
| `ApiExceptionHandler` | Maps exceptions to HTTP statuses. |

## Why some methods return a future

Enqueue, dequeue and ack return a `CompletableFuture`. The web server's thread is released
while the request waits, for example during a long-poll dequeue, and the response is written
when the future completes.
