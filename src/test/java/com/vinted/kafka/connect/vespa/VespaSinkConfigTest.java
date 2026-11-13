package com.vinted.kafka.connect.vespa;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VespaSinkConfigTest {
    private final Map<String, String> params = new HashMap<>();

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
