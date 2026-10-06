# transport: broker-to-broker calls over gRPC

Brokers in different processes talk to each other through this package. It carries the
`BrokerApi` interface from [cluster](../cluster/README.md) across the network and nothing else:
there is no queue logic here, and clients never see it. They use the REST API.

```
  broker A                                                broker B
  Peers ──▶ GrpcBrokerTransport.connect(B) ──▶ RemoteBroker ── gRPC ──▶ GrpcBrokerServer ──▶ Broker.LocalApi
            one channel per broker address                                 on dpq.advertised.grpc-port
```

## What goes over the wire

Defined in [broker.proto](../../../../../proto/broker.proto), which compiles into the `Wire`
classes and `BrokerServiceGrpc`:

| Call | Shape | Used for |
|---|---|---|
| `Enqueue`, `Dequeue`, `Ack`, `Metrics`, `DeadLetters` | One request, one reply | Requests a broker passes on to the shard's leader. Each names the leader epoch the sender believes in. |
| `ReadySummary` | One request, one reply | The highest ready priority of every shard the broker leads, for dequeue routing. Sent every 200 ms. |
| `Fetch` | A stream each way, one per shard | Replication. The follower opens it and reports its log end; the leader sends batches of log entries and the high watermark until it ends the stream. |

Every shard's fetch stream to the same broker shares one connection, so a cluster of N brokers
keeps at most N − 1 connections per broker however many shards there are.

Log entries travel as protobuf: the offset, the epoch, and the event with every time value the
leader stamped into it, so a follower never reads its own clock.

## Errors

The service's own exceptions cross the wire as the same exception on the other side. The kind
of error (`NOT_LEADER`, `NOT_ENOUGH_REPLICAS`, `COMMIT_TIMEOUT`, `SHARD_UNAVAILABLE`,
`INVALID_ARGUMENT`) travels in a gRPC header called `dpq-error`, because a status code alone
cannot tell "not the leader" from "not enough replicas", and the caller must react differently
to each. A call that fails without that header did not reach a working broker, and becomes
`BrokerUnreachableException`.

## The classes

| Class | What it is |
|---|---|
| `GrpcBrokerServer` | Serves this broker's `BrokerApi` to the others. Started only when `dpq.coordinator.type=etcd`. |
| `GrpcBrokerTransport` | The client side: one channel per broker address, and a `BrokerApi` per broker that sends the calls over it. |
| `WireMapper` | Converts between the broker's own types and their protobuf form. |
| `WireErrors` | Puts an exception's kind in the header on the way out and rebuilds the exception on the way in. |
| `Wire`, `BrokerServiceGrpc` | Generated from the proto at build time. |

Messages can be up to 64 MB, which is large enough for a batch of 500 log entries with sizeable
payloads. A passed-on request may take the leader's commit timeout plus five seconds before the
caller gives up on it.
