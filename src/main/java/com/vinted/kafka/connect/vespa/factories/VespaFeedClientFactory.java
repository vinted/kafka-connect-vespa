package com.vinted.kafka.connect.vespa.factories;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedClientBuilder;
import ai.vespa.feed.client.impl.GracePeriodCircuitBreaker;
import com.google.common.collect.ImmutableList;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;

import java.time.Duration;

public class VespaFeedClientFactory {
    public static FeedClient create(VespaSinkConfig config) {
        FeedClientBuilder builder = FeedClientBuilder.create(ImmutableList.copyOf(config.urls))
                .setConnectionsPerEndpoint(config.connectionsPerEndpoint)
                .setMaxStreamPerConnection(config.maxStreamsPerConnection)
                .setDryrun(config.dryrun)
                .setSpeedTest(config.speedTest)
                .setCircuitBreaker(new GracePeriodCircuitBreaker(Duration.ofSeconds(10), config.maxFailureDuration))
                .setRetryStrategy(new FeedClient.RetryStrategy() {
                    @Override
                    public boolean retry(FeedClient.OperationType type) {
                        return config.retryStrategyOperationsTypes.contains(type);
                    }

                    @Override
                    public int retries() {
                        return config.retryStrategyRetries;
                    }
                });

        if (config.vespaCloudToken != null) {
            builder.addRequestHeader("Authorization", "Bearer " + config.vespaCloudToken);
        }

        return builder.build();
    }
}
