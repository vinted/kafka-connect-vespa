package com.vinted.kafka.connect.vespa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginUtils;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.Test;

/**
 * Loads the Confluent Hub package the build writes in a plugin class loader, as a Connect worker
 * does, isolated from a classpath that holds its own Kafka Connect and jackson. Failsafe runs it
 * once the package is built.
 */
class ConfluentHubPackageTest {
  private static final Path PLUGIN_PATH = Paths.get(System.getProperty("plugin.path"));

  @Test
  void providesTheConnectorFromThePackage() throws Exception {
    try (PluginClassLoader plugin = pluginLoader()) {
      SinkConnector connector = newInstance(plugin, VespaSinkConnector.class.getName());

      assertTrue(
          connector.version().matches("\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?"), connector.version());
    }
  }

  @Test
  void convertsAvroWithTheBundledConverter() throws Exception {
    try (PluginClassLoader plugin = pluginLoader()) {
      Converter converter = newInstance(plugin, "io.confluent.connect.avro.AvroConverter");
      ClassLoader previous = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(plugin);
      try {
        converter.configure(Map.of("schema.registry.url", "mock://package"), false);
        byte[] avro = converter.fromConnectData("topic", Schema.STRING_SCHEMA, "value");
        SchemaAndValue converted = converter.toConnectData("topic", avro);

        assertEquals("value", converted.value());
      } finally {
        Thread.currentThread().setContextClassLoader(previous);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T newInstance(PluginClassLoader plugin, String className) throws Exception {
    Class<?> type = plugin.loadClass(className);
    assertSame(plugin, type.getClassLoader(), className + " must come from the package");
    return (T) type.getDeclaredConstructor().newInstance();
  }

  private static PluginClassLoader pluginLoader() throws IOException {
    List<Path> locations = PluginUtils.pluginLocations(PLUGIN_PATH);
    assertEquals(1, locations.size(), locations.toString());

    List<URL> jars = new ArrayList<>();
    for (Path jar : PluginUtils.pluginUrls(locations.get(0))) {
      jars.add(url(jar));
    }
    return new PluginClassLoader(
        url(locations.get(0)),
        jars.toArray(new URL[0]),
        ConfluentHubPackageTest.class.getClassLoader());
  }

  private static URL url(Path path) throws MalformedURLException {
    return path.toUri().toURL();
  }
}
