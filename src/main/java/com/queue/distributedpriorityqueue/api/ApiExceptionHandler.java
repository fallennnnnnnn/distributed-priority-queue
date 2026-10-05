package com.queue.distributedpriorityqueue.api;

import com.queue.distributedpriorityqueue.exception.BrokerUnreachableException;
import com.queue.distributedpriorityqueue.exception.CommitTimeoutException;
import com.queue.distributedpriorityqueue.exception.MessageNotFoundException;
import com.queue.distributedpriorityqueue.exception.NotEnoughReplicasException;
import com.queue.distributedpriorityqueue.exception.NotLeaderException;
import com.queue.distributedpriorityqueue.exception.QueueAlreadyExistsException;
import com.queue.distributedpriorityqueue.exception.QueueNotFoundException;
import com.queue.distributedpriorityqueue.exception.ShardUnavailableException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Turns the service's exceptions into HTTP errors with a problem-detail body. */
@RestControllerAdvice
class ApiExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail badRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler({QueueNotFoundException.class, MessageNotFoundException.class})
    ProblemDetail notFound(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
    }

    @ExceptionHandler(QueueAlreadyExistsException.class)
    ProblemDetail conflict(QueueAlreadyExistsException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
    }

    /** The request was sound but the cluster cannot serve it right now; the caller may retry. */
    @ExceptionHandler({ShardUnavailableException.class, NotLeaderException.class, NotEnoughReplicasException.class,
            CommitTimeoutException.class, BrokerUnreachableException.class})
    ProblemDetail unavailable(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }
}
