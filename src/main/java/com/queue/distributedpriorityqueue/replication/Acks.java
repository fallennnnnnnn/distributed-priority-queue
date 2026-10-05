package com.queue.distributedpriorityqueue.replication;

/** How much confirmation an enqueue waits for. */
public enum Acks {

    /** Answer once the leader has the message. Faster, but the message is lost if the leader fails right after. */
    LEADER,

    /** Answer once every in-sync replica has the message. */
    ALL
}
