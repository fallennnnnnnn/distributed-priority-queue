package com.queue.distributedpriorityqueue.transport;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.queue.distributedpriorityqueue.cluster.BrokerApi;
import com.queue.distributedpriorityqueue.cluster.BrokerTransport;
import com.queue.distributedpriorityqueue.coordination.BrokerInfo;
import com.queue.distributedpriorityqueue.coordination.ShardId;
import com.queue.distributedpriorityqueue.core.MessageId;
import com.queue.distributedpriorityqueue.core.MessageView;
import com.queue.distributedpriorityqueue.core.ShardMetrics;
import com.queue.distributedpriorityqueue.replication.Acks;
import com.queue.distributedpriorityqueue.replication.FetchEnd;
import com.queue.distributedpriorityqueue.replication.FetchListener;
import com.queue.distributedpriorityqueue.replication.FetchRequest;
import com.queue.distributedpriorityqueue.replication.FetchStream;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Reaches other brokers over gRPC. One connection is kept per broker address, and every shard's
 * replication stream to that broker shares it.
 */
public final class GrpcBrokerTransport implements BrokerTransport {

    private final Duration callTimeout;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    /** @param callTimeout how long a passed-on request may take; it should outlast the leader's commit timeout */
    public GrpcBrokerTransport(Duration callTimeout) {
        this.callTimeout = callTimeout;
    }

    @Override
    public BrokerApi connect(BrokerInfo target) {
        ManagedChannel channel = channels.computeIfAbsent(target.host() + ":" + target.grpcPort(),
                address -> NettyChannelBuilder.forAddress(target.host(), target.grpcPort())
                        .usePlaintext()
                        .maxInboundMessageSize(GrpcBrokerServer.MAX_MESSAGE_BYTES)
                        .build());
        return new RemoteBroker(target.id(), channel);
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }

    private final class RemoteBroker implements BrokerApi {

        private final String brokerId;
        private final ManagedChannel channel;

        RemoteBroker(String brokerId, ManagedChannel channel) {
            this.brokerId = brokerId;
            this.channel = channel;
        }

        @Override
        public CompletableFuture<MessageId> enqueue(ShardId shard, int epoch, String payload, String priority,
                Duration ttl, Acks acks) {
            Wire.EnqueueRequest.Builder request = Wire.EnqueueRequest.newBuilder()
                    .setShard(WireMapper.toWire(shard)).setEpoch(epoch).setPayload(payload)
                    .setAcks(WireMapper.toWire(acks));
            if (priority != null) {
                request.setPriority(priority);
            }
            if (ttl != null) {
                request.setTtlNanos(ttl.toNanos());
            }
            return call(calls().enqueue(request.build()), reply -> WireMapper.fromWire(reply.getId()));
        }

        @Override
        public CompletableFuture<Optional<MessageView>> dequeue(ShardId shard, int epoch) {
            return call(calls().dequeue(shardRequest(shard, epoch)), reply -> reply.hasMessage()
                    ? Optional.of(WireMapper.fromWire(reply.getMessage()))
                    : Optional.empty());
        }

        @Override
        public CompletableFuture<Boolean> ack(ShardId shard, int epoch, MessageId messageId) {
            Wire.AckRequest request = Wire.AckRequest.newBuilder().setShard(WireMapper.toWire(shard)).setEpoch(epoch)
                    .setId(WireMapper.toWire(messageId)).build();
            return call(calls().ack(request), Wire.AckReply::getAcked);
        }

        @Override
        public CompletableFuture<ShardMetrics> metrics(ShardId shard, int epoch) {
            return call(calls().metrics(shardRequest(shard, epoch)), WireMapper::fromWire);
        }

        @Override
        public CompletableFuture<List<MessageView>> deadLetters(ShardId shard, int epoch) {
            return call(calls().deadLetters(shardRequest(shard, epoch)),
                    reply -> reply.getMessagesList().stream().map(WireMapper::fromWire).toList());
        }

        @Override
        public CompletableFuture<Map<ShardId, Integer>> readySummary() {
            return call(calls().readySummary(Wire.ReadySummaryRequest.getDefaultInstance()), reply -> {
                Map<ShardId, Integer> summary = new HashMap<>();
                reply.getShardsList().forEach(
                        shard -> summary.put(WireMapper.fromWire(shard.getShard()), shard.getHighestReadyRank()));
                return summary;
            });
        }

        @Override
        public FetchStream openFetch(FetchRequest request, FetchListener toFollower) {
            AtomicBoolean ended = new AtomicBoolean();
            StreamObserver<Wire.FetchUp> toLeader = BrokerServiceGrpc.newStub(channel).fetch(new StreamObserver<>() {
                @Override
                public void onNext(Wire.FetchDown fromLeader) {
                    if (fromLeader.hasEntries()) {
                        toFollower.onEntries(
                                fromLeader.getEntries().getEntriesList().stream().map(WireMapper::fromWire).toList(),
                                fromLeader.getEntries().getHighWatermark());
                    } else if (fromLeader.hasEnd() && ended.compareAndSet(false, true)) {
                        toFollower.onEnd(WireMapper.fromWire(fromLeader.getEnd()));
                    }
                }

                @Override
                public void onError(Throwable error) {
                    onCompleted();
                }

                @Override
                public void onCompleted() {
                    if (ended.compareAndSet(false, true)) {
                        toFollower.onEnd(FetchEnd.of(FetchEnd.Reason.CLOSED));
                    }
                }
            });
            toLeader.onNext(Wire.FetchUp.newBuilder().setOpen(WireMapper.toWire(request)).build());
            return new FetchStream() {
                private boolean closed;

                @Override
                public synchronized void reportLogEnd(long logEndOffset) {
                    if (!closed) {
                        try {
                            toLeader.onNext(Wire.FetchUp.newBuilder().setLogEnd(logEndOffset).build());
                        } catch (RuntimeException e) {
                            // The stream has already failed; the follower is told through onEnd.
                        }
                    }
                }

                @Override
                public synchronized void close() {
                    if (!closed) {
                        closed = true;
                        try {
                            toLeader.onCompleted();
                        } catch (RuntimeException e) {
                            // Already over.
                        }
                    }
                }
            };
        }

        private BrokerServiceGrpc.BrokerServiceFutureStub calls() {
            return BrokerServiceGrpc.newFutureStub(channel).withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        private Wire.ShardRequest shardRequest(ShardId shard, int epoch) {
            return Wire.ShardRequest.newBuilder().setShard(WireMapper.toWire(shard)).setEpoch(epoch).build();
        }

        private <W, T> CompletableFuture<T> call(ListenableFuture<W> reply, Function<W, T> fromWire) {
            CompletableFuture<T> result = new CompletableFuture<>();
            Futures.addCallback(reply, new FutureCallback<>() {
                @Override
                public void onSuccess(W wire) {
                    try {
                        result.complete(fromWire.apply(wire));
                    } catch (RuntimeException e) {
                        result.completeExceptionally(e);
                    }
                }

                @Override
                public void onFailure(Throwable error) {
                    result.completeExceptionally(WireErrors.fromWire(error, brokerId));
                }
            }, MoreExecutors.directExecutor());
            return result;
        }
    }
}
