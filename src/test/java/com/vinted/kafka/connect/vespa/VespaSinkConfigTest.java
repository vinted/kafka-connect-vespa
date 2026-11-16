package com.vinted.kafka.connect.vespa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class VespaSinkConfigTest {
  private final Map<String, String> params = new HashMap<>();

  @Test
  void masksVespaCloudToken() {
    params.put("vespa.cloud.token", "vespa_cloud_token_123");
    VespaSinkConfig config = new VespaSinkConfig(params);

    assertEquals(
        ConfigDef.Type.PASSWORD, VespaSinkConfig.CONFIG.configKeys().get("vespa.cloud.token").type);
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
  void acceptsOperationalModeAndBehaviorOnMalformedDocumentsInAnyCase() {
    params.put(VespaSinkConfig.OPERATIONAL_MODE_CONFIG, "raw");
    params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "Warn");
    VespaSinkConfig config = new VespaSinkConfig(params);

    assertEquals(VespaSinkConfig.OperationalMode.RAW, config.operationalMode());
    assertEquals(VespaSinkConfig.BehaviorOnMalformedDoc.WARN, config.behaviorOnMalformedDoc());
  }

  @Test
  void rejectsInvalidValues() {
    assertInvalid("vespa.operational.mode", "rawr");
    assertInvalid("vespa.behavior.on.malformed.documents", "skip");
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
