package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedClient;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class VespaSinkConfigTest {
    private final Map<String, String> params = new HashMap<>();

    @Test
    void createsDefaultRetryStrategy() {
        VespaSinkConfig config = new VespaSinkConfig(params);

        assertEquals(10, config.retryStrategyRetries);
        assertIterableEquals(Arrays.asList(FeedClient.OperationType.PUT, FeedClient.OperationType.UPDATE, FeedClient.OperationType.REMOVE), config.retryStrategyOperationsTypes);
    }

    @Test
    void overridesRetryStrategy() {
        params.put("vespa.retry.strategy.retries", "20");
        params.put("vespa.retry.strategy.operation.types", "UPDATE,REMOVE");

        VespaSinkConfig config = new VespaSinkConfig(params);

        assertEquals(20, config.retryStrategyRetries);
        assertIterableEquals(Arrays.asList(FeedClient.OperationType.UPDATE, FeedClient.OperationType.REMOVE), config.retryStrategyOperationsTypes);
    }
}
