package com.queue.distributedpriorityqueue.exception;

/** The coordinator could not be reached. Without it a broker cannot confirm what it leads. */
public class CoordinatorUnavailableException extends RuntimeException {

    public CoordinatorUnavailableException(String message) {
        super(message);
    }
}
