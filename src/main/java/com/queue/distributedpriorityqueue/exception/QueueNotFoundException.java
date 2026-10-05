package com.queue.distributedpriorityqueue.exception;

public class QueueNotFoundException extends RuntimeException {

    public QueueNotFoundException(String queue) {
        super("no queue named " + queue);
    }
}
