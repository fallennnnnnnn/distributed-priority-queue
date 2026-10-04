package com.queue.distributedpriorityqueue;

import org.springframework.boot.SpringApplication;

public class TestDistributedPriorityQueueApplication {

    public static void main(String[] args) {
        SpringApplication.from(DistributedPriorityQueueApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
