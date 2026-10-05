package com.queue.distributedpriorityqueue.transport;

import com.queue.distributedpriorityqueue.exception.BrokerUnreachableException;
import com.queue.distributedpriorityqueue.exception.CommitTimeoutException;
import com.queue.distributedpriorityqueue.exception.NotEnoughReplicasException;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.exception.ShardUnavailableException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.CompletionException;

/**
 * Carries the service's own errors across gRPC, so that the caller on the other side sees the
 * same exception the serving broker threw. The kind of error travels in a header, because a gRPC
 * status code alone cannot tell, for example, "not the leader" from "not enough replicas".
 */
final class WireErrors {

    private static final Metadata.Key<String> KIND = Metadata.Key.of("dpq-error", Metadata.ASCII_STRING_MARSHALLER);

    private WireErrors() {
    }

    static StatusRuntimeException toWire(Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        Kind kind = Kind.of(cause);
        Metadata headers = new Metadata();
        if (kind != null) {
            headers.put(KIND, kind.name());
        }
        Status status = kind == null ? Status.INTERNAL : kind.status;
        return status.withDescription(cause.getMessage()).asRuntimeException(headers);
    }

    /** @param brokerId the broker that was called, named when the failure is that it could not be reached */
    static RuntimeException fromWire(Throwable error, String brokerId) {
        if (!(error instanceof StatusRuntimeException status)) {
            return error instanceof RuntimeException runtime ? runtime : new RuntimeException(error);
        }
        String kind = status.getTrailers() == null ? null : status.getTrailers().get(KIND);
        if (kind == null) {
            // Not one of our errors: the call itself failed, so the broker is as good as unreachable.
            return new BrokerUnreachableException(brokerId);
        }
        String message = status.getStatus().getDescription();
        return switch (Kind.valueOf(kind)) {
            case NOT_LEADER -> new NotLeaderException(message);
            case NOT_ENOUGH_REPLICAS -> new NotEnoughReplicasException(message);
            case COMMIT_TIMEOUT -> new CommitTimeoutException(message);
            case SHARD_UNAVAILABLE -> new ShardUnavailableException(message);
            case INVALID_ARGUMENT -> new IllegalArgumentException(message);
        };
    }

    private enum Kind {
        NOT_LEADER(Status.FAILED_PRECONDITION),
        NOT_ENOUGH_REPLICAS(Status.UNAVAILABLE),
        COMMIT_TIMEOUT(Status.DEADLINE_EXCEEDED),
        SHARD_UNAVAILABLE(Status.UNAVAILABLE),
        INVALID_ARGUMENT(Status.INVALID_ARGUMENT);

        private final Status status;

        Kind(Status status) {
            this.status = status;
        }

        static Kind of(Throwable error) {
            if (error instanceof NotLeaderException) {
                return NOT_LEADER;
            } else if (error instanceof NotEnoughReplicasException) {
                return NOT_ENOUGH_REPLICAS;
            } else if (error instanceof CommitTimeoutException) {
                return COMMIT_TIMEOUT;
            } else if (error instanceof ShardUnavailableException) {
                return SHARD_UNAVAILABLE;
            } else if (error instanceof IllegalArgumentException) {
                return INVALID_ARGUMENT;
            }
            return null;
        }
    }
}
