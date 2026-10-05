package com.queue.distributedpriorityqueue.transport;

import com.queue.distributedpriorityqueue.cluster.BrokerApi;
import com.queue.distributedpriorityqueue.log.LogEntry;
import com.queue.distributedpriorityqueue.replication.FetchEnd;
import com.queue.distributedpriorityqueue.replication.FetchListener;
import com.queue.distributedpriorityqueue.replication.FetchStream;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serves a broker's {@link BrokerApi} to the other brokers over gRPC. */
public final class GrpcBrokerServer implements AutoCloseable {

    /** Large enough for a batch of log entries with sizeable payloads. */
    static final int MAX_MESSAGE_BYTES = 64 * 1024 * 1024;

    private static final Logger log = LoggerFactory.getLogger(GrpcBrokerServer.class);

    private final Server server;

    /** @param port the port to listen on; 0 picks a free one */
    public GrpcBrokerServer(BrokerApi broker, int port) {
        try {
            this.server = NettyServerBuilder.forPort(port)
                    .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                    .addService(new Service(broker))
                    .build()
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException("could not listen for other brokers on port " + port, e);
        }
        log.info("Listening for other brokers on port {}", server.getPort());
    }

    public int port() {
        return server.getPort();
    }

    @Override
    public void close() {
        server.shutdownNow();
    }

    private static final class Service extends BrokerServiceGrpc.BrokerServiceImplBase {

        private final BrokerApi broker;

        Service(BrokerApi broker) {
            this.broker = broker;
        }

        @Override
        public void enqueue(Wire.EnqueueRequest request, StreamObserver<Wire.EnqueueReply> reply) {
            answer(reply, broker.enqueue(WireMapper.fromWire(request.getShard()), request.getEpoch(),
                            request.getPayload(), request.hasPriority() ? request.getPriority() : null,
                            request.hasTtlNanos() ? Duration.ofNanos(request.getTtlNanos()) : null,
                            WireMapper.fromWire(request.getAcks())),
                    id -> Wire.EnqueueReply.newBuilder().setId(WireMapper.toWire(id)).build());
        }

        @Override
        public void dequeue(Wire.ShardRequest request, StreamObserver<Wire.DequeueReply> reply) {
            answer(reply, broker.dequeue(WireMapper.fromWire(request.getShard()), request.getEpoch()), message -> {
                Wire.DequeueReply.Builder wire = Wire.DequeueReply.newBuilder();
                message.ifPresent(present -> wire.setMessage(WireMapper.toWire(present)));
                return wire.build();
            });
        }

        @Override
        public void ack(Wire.AckRequest request, StreamObserver<Wire.AckReply> reply) {
            answer(reply, broker.ack(WireMapper.fromWire(request.getShard()), request.getEpoch(),
                            WireMapper.fromWire(request.getId())),
                    acked -> Wire.AckReply.newBuilder().setAcked(acked).build());
        }

        @Override
        public void metrics(Wire.ShardRequest request, StreamObserver<Wire.MetricsReply> reply) {
            answer(reply, broker.metrics(WireMapper.fromWire(request.getShard()), request.getEpoch()),
                    WireMapper::toWire);
        }

        @Override
        public void deadLetters(Wire.ShardRequest request, StreamObserver<Wire.DeadLettersReply> reply) {
            answer(reply, broker.deadLetters(WireMapper.fromWire(request.getShard()), request.getEpoch()),
                    messages -> Wire.DeadLettersReply.newBuilder()
                            .addAllMessages(messages.stream().map(WireMapper::toWire).toList()).build());
        }

        @Override
        public void readySummary(Wire.ReadySummaryRequest request, StreamObserver<Wire.ReadySummaryReply> reply) {
            answer(reply, broker.readySummary(), summary -> {
                Wire.ReadySummaryReply.Builder wire = Wire.ReadySummaryReply.newBuilder();
                summary.forEach((shard, rank) -> wire.addShards(
                        Wire.ShardRank.newBuilder().setShard(WireMapper.toWire(shard)).setHighestReadyRank(rank)));
                return wire.build();
            });
        }

        @Override
        public StreamObserver<Wire.FetchUp> fetch(StreamObserver<Wire.FetchDown> toFollower) {
            return new FetchCall(broker, toFollower);
        }

        private static <T, W> void answer(StreamObserver<W> reply, CompletableFuture<T> result, Function<T, W> toWire) {
            result.whenComplete((value, error) -> {
                if (error != null) {
                    reply.onError(WireErrors.toWire(error));
                    return;
                }
                try {
                    reply.onNext(toWire.apply(value));
                    reply.onCompleted();
                } catch (RuntimeException e) {
                    reply.onError(WireErrors.toWire(e));
                }
            });
        }
    }

    /**
     * One follower's replication stream. The first message from the follower opens it; every
     * later one reports how far the follower's log reaches.
     */
    private static final class FetchCall implements StreamObserver<Wire.FetchUp> {

        private final BrokerApi broker;
        private final StreamObserver<Wire.FetchDown> toFollower;
        private FetchStream stream;
        private boolean finished;

        FetchCall(BrokerApi broker, StreamObserver<Wire.FetchDown> toFollower) {
            this.broker = broker;
            this.toFollower = toFollower;
        }

        @Override
        public void onNext(Wire.FetchUp fromFollower) {
            if (fromFollower.hasOpen() && stream == null) {
                stream = broker.openFetch(WireMapper.fromWire(fromFollower.getOpen()), new FetchListener() {
                    @Override
                    public void onEntries(List<LogEntry> entries, long highWatermark) {
                        send(Wire.FetchDown.newBuilder().setEntries(Wire.Entries.newBuilder()
                                .addAllEntries(entries.stream().map(WireMapper::toWire).toList())
                                .setHighWatermark(highWatermark)).build(), false);
                    }

                    @Override
                    public void onEnd(FetchEnd end) {
                        send(Wire.FetchDown.newBuilder().setEnd(WireMapper.toWire(end)).build(), true);
                    }
                });
            } else if (fromFollower.hasLogEnd() && stream != null) {
                stream.reportLogEnd(fromFollower.getLogEnd());
            }
        }

        @Override
        public void onError(Throwable error) {
            followerLeft();
        }

        @Override
        public void onCompleted() {
            followerLeft();
        }

        private void followerLeft() {
            if (stream != null) {
                stream.close();
            }
            synchronized (this) {
                if (!finished) {
                    finished = true;
                    try {
                        toFollower.onCompleted();
                    } catch (RuntimeException e) {
                        // The call is already over.
                    }
                }
            }
        }

        /** The leader sends from whichever thread holds its shard lock, so sends are serialised here. */
        private synchronized void send(Wire.FetchDown message, boolean last) {
            if (finished) {
                return;
            }
            try {
                toFollower.onNext(message);
                if (last) {
                    finished = true;
                    toFollower.onCompleted();
                }
            } catch (RuntimeException e) {
                finished = true;
            }
        }
    }
}
