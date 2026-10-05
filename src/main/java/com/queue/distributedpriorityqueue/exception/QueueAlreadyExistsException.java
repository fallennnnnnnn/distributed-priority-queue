package com.queue.distributedpriorityqueue.exception;

public class QueueAlreadyExistsException extends RuntimeException {

    public QueueAlreadyExistsException(String queue) {
        super("a queue named " + queue + " already exists");
    }
}
