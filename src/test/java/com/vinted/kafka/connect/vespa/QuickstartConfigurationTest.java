package com.vinted.kafka.connect.vespa;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Every configuration in {@code config/}, which README.md points users to, is valid. */
class QuickstartConfigurationTest {
  private static final Path CONFIG = Paths.get("config");
  private static final ObjectMapper JSON = new ObjectMapper();

  static Stream<Path> quickstarts() throws IOException {
    try (Stream<Path> files = Files.list(CONFIG)) {
      return files.sorted().collect(Collectors.toList()).stream();
    }
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("quickstarts")
  void isValid(Path quickstart) throws IOException {
    Map<String, String> props = props(quickstart);

    assertEquals(Map.of(), errors(new VespaSinkConnector().validate(props).configValues()));
    assertEquals(List.of(), unknownConnectorSettings(props));
  }

  private static Map<String, List<String>> errors(List<ConfigValue> values) {
    return values.stream()
        .filter(value -> !value.errorMessages().isEmpty())
        .collect(Collectors.toMap(ConfigValue::name, ConfigValue::errorMessages));
  }

  private static List<String> unknownConnectorSettings(Map<String, String> props) {
    return props.keySet().stream()
        .filter(name -> name.startsWith("vespa."))
        .filter(name -> !VespaSinkConfig.CONFIG.names().contains(name))
        .sorted()
        .collect(Collectors.toList());
  }

  private static Map<String, String> props(Path quickstart) throws IOException {
    if (quickstart.toString().endsWith(".json")) {
      Map<?, ?> connector = JSON.readValue(quickstart.toFile(), Map.class);
      return strings((Map<?, ?>) connector.get("config"));
    }

    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(quickstart)) {
      properties.load(reader);
    }
    return strings(properties);
  }

  private static Map<String, String> strings(Map<?, ?> values) {
    Map<String, String> strings = new HashMap<>();
    values.forEach((name, value) -> strings.put(String.valueOf(name), String.valueOf(value)));
    return strings;
  }
}
