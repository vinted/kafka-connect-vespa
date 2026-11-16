package com.vinted.kafka.connect.vespa;

import static com.vinted.kafka.connect.vespa.VespaSinkConfig.SSL_CERTIFICATE_PATH_CONFIG;
import static com.vinted.kafka.connect.vespa.VespaSinkConfig.SSL_PRIVATE_KEY_PATH_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

class VespaSinkConnectorTest {
  private static final String TOGETHER =
      "'vespa.ssl.certificate.path' and 'vespa.ssl.private.key.path' must be set together.";

  private final VespaSinkConnector connector = new VespaSinkConnector();

  @Test
  void reportsNoErrorsForValidConfiguration() {
    Config config =
        connector.validate(
            Map.of(
                SSL_CERTIFICATE_PATH_CONFIG, "/etc/vespa/cert.pem",
                SSL_PRIVATE_KEY_PATH_CONFIG, "/etc/vespa/key.pem"));

    assertEquals(Map.of(), errors(config));
  }

  @Test
  void reportsCertificateWithoutPrivateKeyOnThePrivateKey() {
    Config config = connector.validate(Map.of(SSL_CERTIFICATE_PATH_CONFIG, "/etc/vespa/cert.pem"));

    assertEquals(Map.of(SSL_PRIVATE_KEY_PATH_CONFIG, List.of(TOGETHER)), errors(config));
  }

  @Test
  void reportsPrivateKeyWithoutCertificateOnTheCertificate() {
    Config config = connector.validate(Map.of(SSL_PRIVATE_KEY_PATH_CONFIG, "/etc/vespa/key.pem"));

    assertEquals(Map.of(SSL_CERTIFICATE_PATH_CONFIG, List.of(TOGETHER)), errors(config));
  }

  @Test
  void failsToStartWithAnInvalidConfiguration() {
    Map<String, String> props = Map.of(SSL_CERTIFICATE_PATH_CONFIG, "/etc/vespa/cert.pem");

    ConfigException thrown = assertThrows(ConfigException.class, () -> connector.start(props));
    assertEquals(
        "Invalid value null for configuration " + SSL_PRIVATE_KEY_PATH_CONFIG + ": " + TOGETHER,
        thrown.getMessage());
  }

  @Test
  void givesEveryTaskTheConnectorConfiguration() {
    Map<String, String> props = Map.of(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
    connector.start(props);

    assertEquals(List.of(props, props, props), connector.taskConfigs(3));
  }

  private static Map<String, List<String>> errors(Config config) {
    return config.configValues().stream()
        .filter(value -> !value.errorMessages().isEmpty())
        .collect(Collectors.toMap(ConfigValue::name, ConfigValue::errorMessages));
  }
}
