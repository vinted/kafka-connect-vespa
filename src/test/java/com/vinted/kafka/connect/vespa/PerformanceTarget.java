package com.vinted.kafka.connect.vespa;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.vinted.kafka.connect.vespa.mocks.FakeFeedClient;
import com.vinted.kafka.connect.vespa.mocks.MockVespaServer;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * What the performance tests feed, chosen by {@code vespa.perf.endpoint}: a running Vespa at a URL,
 * {@code mock} for a local server that acknowledges every operation (after {@code
 * vespa.perf.mock.latency.micros}), or {@code stub} for a feed client that succeeds every operation
 * in memory. Any {@code vespa.perf.connector.<setting>} system property is a connector setting.
 */
final class PerformanceTarget implements AutoCloseable {
  private static final String CONNECTOR_PROPERTY = "vespa.perf.connector.";

  private final Kind kind;
  private final String endpoint;
  private final MockVespaServer mock;
  private final VespaAdmin vespa;

  private enum Kind {
    VESPA,
    MOCK,
    STUB
  }

  private PerformanceTarget(Kind kind, String endpoint, MockVespaServer mock) {
    this.kind = kind;
    this.endpoint = endpoint;
    this.mock = mock;
    this.vespa = kind == Kind.VESPA ? new VespaAdmin(endpoint) : null;
  }

  /** The target named by {@code vespa.perf.endpoint}, or {@code null} when it is not set. */
  static PerformanceTarget fromSystemProperties() throws Exception {
    String endpoint = System.getProperty("vespa.perf.endpoint");
    if (endpoint == null) {
      return null;
    }
    if (endpoint.equals("stub")) {
      return new PerformanceTarget(Kind.STUB, "http://localhost:1", null);
    }
    if (endpoint.equals("mock")) {
      Duration latency = Duration.ofNanos(Long.getLong("vespa.perf.mock.latency.micros", 0) * 1000);
      MockVespaServer mock = MockVespaServer.http(MockVespaServer.acknowledgeAfter(latency));
      return new PerformanceTarget(Kind.MOCK, mock.url(), mock);
    }
    if (endpoint.startsWith("http")) {
      return new PerformanceTarget(Kind.VESPA, endpoint, null);
    }
    throw new IllegalArgumentException(
        "vespa.perf.endpoint must be a URL, mock or stub: " + endpoint);
  }

  /** Whether fed documents land in Vespa, where they can be counted. */
  boolean indexes() {
    Map<String, String> props = connectorProps(Map.of());
    return kind == Kind.VESPA
        && !Boolean.parseBoolean(props.get(VespaSinkConfig.DRYRUN_CONFIG))
        && !Boolean.parseBoolean(props.get(VespaSinkConfig.SPEED_TEST_CONFIG));
  }

  /** Whether the target is the connector's own feed client, rather than a stand-in for it. */
  boolean usesFeedClient() {
    return kind != Kind.STUB;
  }

  /** How often to feed the same records: only where nothing is indexed can they be fed again. */
  int passes() {
    return indexes() ? 1 : Integer.getInteger("vespa.perf.passes", 1);
  }

  /**
   * Settings of a connector feeding the target the {@link PerformanceRecords}, with the overrides
   * on top.
   */
  Map<String, String> connectorProps(Map<String, String> overrides) {
    Map<String, String> props = new HashMap<>();
    props.put(VespaSinkConfig.ENDPOINT_CONFIG, endpoint);
    props.put(VespaSinkConfig.NAMESPACE_CONFIG, PerformanceRecords.NAMESPACE);
    props.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, PerformanceRecords.DOCUMENT_TYPE);
    System.getProperties().stringPropertyNames().stream()
        .filter(name -> name.startsWith(CONNECTOR_PROPERTY))
        .forEach(
            name ->
                props.put(name.substring(CONNECTOR_PROPERTY.length()), System.getProperty(name)));
    props.putAll(overrides);
    return props;
  }

  /** Starts a task feeding the target behind a Connect worker. */
  ConnectWorker start(Map<String, String> overrides) {
    VespaSinkTask task =
        usesFeedClient()
            ? new VespaSinkTask()
            : new VespaSinkTask(config -> FakeFeedClient.acknowledgingAll());
    return new ConnectWorker(task).start(connectorProps(overrides));
  }

  /** Deletes every document fed so far, where documents are kept. */
  void clear() throws IOException, InterruptedException {
    if (vespa != null) {
      vespa.deleteAll();
    }
  }

  /** Asserts how many documents Vespa holds, where it indexes them. */
  void assertIndexed(long documents) throws IOException, InterruptedException {
    if (indexes()) {
      assertEquals(documents, vespa.count());
    }
  }

  /** The Vespa that indexes the documents; only for a target that {@link #indexes()}. */
  VespaAdmin vespa() {
    if (!indexes()) {
      throw new IllegalStateException("the " + kind + " target indexes nothing");
    }
    return vespa;
  }

  @Override
  public void close() throws Exception {
    if (mock != null) {
      mock.close();
    }
  }
}
