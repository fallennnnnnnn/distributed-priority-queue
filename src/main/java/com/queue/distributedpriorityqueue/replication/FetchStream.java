package com.queue.distributedpriorityqueue.replication;

/** The follower's side of a fetch stream, as the leader sees it. */
public interface FetchStream {

    /** A stream that was refused when it was opened. */
    FetchStream REFUSED = new FetchStream() {
        @Override
        public void reportLogEnd(long logEndOffset) {
        }

        @Override
        public void close() {
        }
    };

    /** The follower now has every entry below {@code logEndOffset}. */
    void reportLogEnd(long logEndOffset);

    void close();
}
