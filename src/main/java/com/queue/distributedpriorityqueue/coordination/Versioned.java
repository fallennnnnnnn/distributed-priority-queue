package com.queue.distributedpriorityqueue.coordination;

/** A value read from the coordinator, with the version to quote when replacing it. */
public record Versioned<T>(T value, long version) {
}
