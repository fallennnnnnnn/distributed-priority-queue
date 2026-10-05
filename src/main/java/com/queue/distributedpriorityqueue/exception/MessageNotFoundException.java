package com.queue.distributedpriorityqueue.exception;

/** There is nothing to acknowledge: the message is unknown, was never delivered, or is already gone. */
public class MessageNotFoundException extends RuntimeException {

    public MessageNotFoundException(String queue, String messageId) {
        super("queue " + queue + " has no message " + messageId + " waiting for an acknowledgement");
    }
}
