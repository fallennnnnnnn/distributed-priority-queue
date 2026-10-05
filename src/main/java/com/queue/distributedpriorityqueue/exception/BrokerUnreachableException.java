package com.queue.distributedpriorityqueue.exception;

/** Another broker could not be reached, or stopped answering. */
public class BrokerUnreachableException extends RuntimeException {

    public BrokerUnreachableException(String brokerId) {
        super("broker " + brokerId + " is unreachable");
    }
}
