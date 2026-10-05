package com.queue.distributedpriorityqueue.replication;

import com.queue.distributedpriorityqueue.log.ShardLog.EpochEnd;

/**
 * Why a fetch stream ended.
 *
 * @param leaderEpochEnd only for {@link Reason#DIVERGED}: where the follower's last epoch ends in
 *        the leader's log, which tells the follower how far back to truncate
 */
public record FetchEnd(Reason reason, EpochEnd leaderEpochEnd) {

    public enum Reason {
        /** The broker asked is not the shard's leader. */
        NOT_LEADER,
        /** The follower's epoch is older than the leader's; it must read the shard's state again. */
        FENCED_EPOCH,
        /** The follower's log does not match the leader's at the fetch offset. */
        DIVERGED,
        /** The connection closed. */
        CLOSED
    }

    public static FetchEnd of(Reason reason) {
        return new FetchEnd(reason, null);
    }

    public static FetchEnd diverged(EpochEnd leaderEpochEnd) {
        return new FetchEnd(Reason.DIVERGED, leaderEpochEnd);
    }
}
