package com.queue.distributedpriorityqueue.coordination;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

class InMemoryCoordinatorTest extends CoordinatorContract {

    private InMemoryCoordinator inMemory;

    @BeforeEach
    void start() {
        inMemory = new InMemoryCoordinator();
        coordinator = inMemory;
    }

    @AfterEach
    void close() {
        inMemory.close();
    }

    @Override
    protected void expireLease(String brokerId) {
        inMemory.expireLease(brokerId);
    }
}
