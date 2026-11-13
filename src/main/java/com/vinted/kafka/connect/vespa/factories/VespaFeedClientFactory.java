package com.vinted.kafka.connect.vespa.factories;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedClientBuilder;
import ai.vespa.feed.client.impl.GracePeriodCircuitBreaker;
import com.google.common.collect.ImmutableList;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;

import javax.net.ssl.HostnameVerifier;

public class VespaFeedClientFactory {
    private static final HostnameVerifier NO_HOSTNAME_VERIFICATION = (hostname, session) -> true;

    public static FeedClient create(VespaSinkConfig config) {
        return configure(FeedClientBuilder.create(ImmutableList.copyOf(config.urls)), config).build();
    }

    static FeedClientBuilder configure(FeedClientBuilder builder, VespaSinkConfig config) {
        builder.setConnectionsPerEndpoint(config.connectionsPerEndpoint);
        builder.setMaxStreamPerConnection(config.maxStreamsPerConnection);
        builder.setConnectionTimeToLive(config.connectionTtl);
        builder.setDryrun(config.dryrun);
        builder.setSpeedTest(config.speedTest);
        builder.setCompression(config.compression);
        builder.setInitialInflightFactor(config.initialInflightFactor);
        builder.setCircuitBreaker(
                new GracePeriodCircuitBreaker(config.circuitBreakerGracePeriod, config.maxFailureDuration));
        builder.setRetryStrategy(new FeedClient.RetryStrategy() {
            @Override
            public boolean retry(FeedClient.OperationType type) {
                return config.retryStrategyOperationsTypes.contains(type);
            }

            @Override
            public int retries() {
                return config.retryStrategyRetries;
            }
        });

        if (!config.benchmark) {
            builder.noBenchmarking();
        }

        config.requestHeaders.forEach(builder::addRequestHeader);

        if (config.vespaCloudToken != null) {
            builder.addRequestHeader("Authorization", "Bearer " + config.vespaCloudToken);
        }

        if (config.sslCertificatePath != null) {
            builder.setCertificate(config.sslCertificatePath, config.sslPrivateKeyPath);
        }

        if (config.sslCaCertificatesPath != null) {
            builder.setCaCertificatesFile(config.sslCaCertificatesPath);
        }

        if (!config.sslHostnameVerificationEnabled) {
            builder.setHostnameVerifier(NO_HOSTNAME_VERIFICATION);
        }

        if (config.proxyUrl != null) {
            builder.setProxy(config.proxyUrl);
        }

        if (config.proxyCaCertificatesPath != null) {
            builder.setProxyCaCertificatesFile(config.proxyCaCertificatesPath);
        }

        if (!config.proxyHostnameVerificationEnabled) {
            builder.setProxyHostnameVerifier(NO_HOSTNAME_VERIFICATION);
        }

        config.proxyRequestHeaders.forEach(builder::addProxyRequestHeader);

        return builder;
    }
}
