package com.vinted.kafka.connect.vespa;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The version of the connector and its tasks, read from the properties file the build writes.
 * Unlike the jar manifest, the file is kept in the Confluent Hub package.
 */
public final class VespaSinkConnectorVersion {
  private static final Logger log = LoggerFactory.getLogger(VespaSinkConnectorVersion.class);
  private static final String VERSION_FILE = "/kafka-connect-vespa-version.properties";
  private static final String UNKNOWN = "unknown";
  private static final String VERSION = load();

  private VespaSinkConnectorVersion() {}

  public static String getVersion() {
    return VERSION;
  }

  private static String load() {
    try (InputStream stream = VespaSinkConnectorVersion.class.getResourceAsStream(VERSION_FILE)) {
      if (stream == null) {
        log.warn("{} is missing, so the version is {}.", VERSION_FILE, UNKNOWN);
        return UNKNOWN;
      }

      Properties properties = new Properties();
      properties.load(stream);
      return properties.getProperty("version", UNKNOWN).trim();
    } catch (IOException e) {
      log.warn("Failed to read the version from {}.", VERSION_FILE, e);
      return UNKNOWN;
    }
  }
}
