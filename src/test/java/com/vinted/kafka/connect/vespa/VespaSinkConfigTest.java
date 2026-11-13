package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedClientBuilder;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void createsDefaultVespaCloudToken() {
        VespaSinkConfig config = new VespaSinkConfig(params);

        assertEquals(null, config.vespaCloudToken);
    }

    @Test
    void overridesVespaCloudToken() {
        params.put("vespa.cloud.token", "vespa_cloud_token_123");
        VespaSinkConfig config = new VespaSinkConfig(params);

        assertEquals("vespa_cloud_token_123", config.vespaCloudToken);
    }

    @Test
    void masksVespaCloudToken() {
        params.put("vespa.cloud.token", "vespa_cloud_token_123");
        VespaSinkConfig config = new VespaSinkConfig(params);

        assertEquals(ConfigDef.Type.PASSWORD, VespaSinkConfig.CONFIG.configKeys().get("vespa.cloud.token").type);
        assertFalse(config.values().toString().contains("vespa_cloud_token_123"));
    }

    @Test
    void rejectsEmptyVespaCloudToken() {
        params.put("vespa.cloud.token", "");

        assertThrows(ConfigException.class, () -> new VespaSinkConfig(params));
    }

    @Test
    void createsDefaultFeedClientConfigs() {
        VespaSinkConfig config = new VespaSinkConfig(params);

        assertEquals(Duration.ofSeconds(10), config.circuitBreakerGracePeriod);
        assertEquals(Duration.ZERO, config.connectionTtl);
        assertEquals(FeedClientBuilder.Compression.auto, config.compression);
        assertEquals(1, config.initialInflightFactor);
        assertTrue(config.benchmark);
        assertEquals(Collections.emptyMap(), config.requestHeaders);
        assertNull(config.sslCertificatePath);
        assertNull(config.sslPrivateKeyPath);
        assertNull(config.sslCaCertificatesPath);
        assertTrue(config.sslHostnameVerificationEnabled);
        assertNull(config.proxyUrl);
        assertNull(config.proxyCaCertificatesPath);
        assertTrue(config.proxyHostnameVerificationEnabled);
        assertEquals(Collections.emptyMap(), config.proxyRequestHeaders);
        assertNull(config.condition);
        assertFalse(config.createIfNonExistent);
    }

    @Test
    void overridesFeedClientConfigs() {
        params.put("vespa.circuit.breaker.grace.period.ms", "5000");
        params.put("vespa.connection.ttl.ms", "3600000");
        params.put("vespa.compression", "gzip");
        params.put("vespa.initial.inflight.factor", "4");
        params.put("vespa.benchmark", "false");
        params.put("vespa.request.headers", "X-Team: search, X-Trace:a:b");
        params.put("vespa.ssl.certificate.path", "/certs/client.pem");
        params.put("vespa.ssl.private.key.path", "/certs/client.key");
        params.put("vespa.ssl.ca.certificates.path", "/certs/ca.pem");
        params.put("vespa.ssl.hostname.verification.enabled", "false");
        params.put("vespa.proxy.url", "https://proxy:3128");
        params.put("vespa.proxy.ca.certificates.path", "/certs/proxy-ca.pem");
        params.put("vespa.proxy.hostname.verification.enabled", "false");
        params.put("vespa.proxy.request.headers", "Proxy-Authorization:Basic abc");
        params.put("vespa.operation.condition", "music.locked == false");
        params.put("vespa.operation.create.if.non.existent", "true");

        VespaSinkConfig config = new VespaSinkConfig(params);

        assertEquals(Duration.ofSeconds(5), config.circuitBreakerGracePeriod);
        assertEquals(Duration.ofHours(1), config.connectionTtl);
        assertEquals(FeedClientBuilder.Compression.gzip, config.compression);
        assertEquals(4, config.initialInflightFactor);
        assertFalse(config.benchmark);
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("X-Team", "search");
        headers.put("X-Trace", "a:b");
        assertEquals(headers, config.requestHeaders);
        assertEquals(Paths.get("/certs/client.pem"), config.sslCertificatePath);
        assertEquals(Paths.get("/certs/client.key"), config.sslPrivateKeyPath);
        assertEquals(Paths.get("/certs/ca.pem"), config.sslCaCertificatesPath);
        assertFalse(config.sslHostnameVerificationEnabled);
        assertEquals(URI.create("https://proxy:3128"), config.proxyUrl);
        assertEquals(Paths.get("/certs/proxy-ca.pem"), config.proxyCaCertificatesPath);
        assertFalse(config.proxyHostnameVerificationEnabled);
        assertEquals(Collections.singletonMap("Proxy-Authorization", "Basic abc"), config.proxyRequestHeaders);
        assertEquals("music.locked == false", config.condition);
        assertTrue(config.createIfNonExistent);
    }

    @Test
    void requiresCertificateAndPrivateKeyTogether() {
        params.put("vespa.ssl.certificate.path", "/certs/client.pem");
        assertThrows(ConfigException.class, () -> new VespaSinkConfig(params));

        params.clear();
        params.put("vespa.ssl.private.key.path", "/certs/client.key");
        assertThrows(ConfigException.class, () -> new VespaSinkConfig(params));
    }

    @Test
    void rejectsInvalidValues() {
        assertInvalid("vespa.compression", "zstd");
        assertInvalid("vespa.initial.inflight.factor", "0");
        assertInvalid("vespa.connection.ttl.ms", "-1");
        assertInvalid("vespa.circuit.breaker.grace.period.ms", "-1");
        assertInvalid("vespa.request.headers", "no-separator");
        assertInvalid("vespa.request.headers", ":value");
        assertInvalid("vespa.proxy.request.headers", "no-separator");
        assertInvalid("vespa.proxy.url", "proxy:3128");
        assertInvalid("vespa.proxy.url", "ftp://proxy:3128");
        assertInvalid("vespa.ssl.certificate.path", "");
    }

    private void assertInvalid(String key, String value) {
        Map<String, String> invalid = new HashMap<>();
        invalid.put(key, value);
        assertThrows(ConfigException.class, () -> new VespaSinkConfig(invalid), key + "=" + value);
    }
}
