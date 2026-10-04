package com.queue.distributedpriorityqueue.cluster;

import java.time.Clock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(BrokerSettings.class)
public class BrokerConfiguration {

    /** The one source of time for the broker. Tests replace it with a clock they move by hand. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
