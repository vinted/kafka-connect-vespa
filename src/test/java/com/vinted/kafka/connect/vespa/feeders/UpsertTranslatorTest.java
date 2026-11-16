package com.vinted.kafka.connect.vespa.feeders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParameters;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentOperation;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Put;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Remove;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class UpsertTranslatorTest {
  private static final OperationParameters DEFAULT_PARAMETERS =
      OperationParameters.empty().timeout(Duration.ofMinutes(1));

  private final Map<String, String> params = new HashMap<>();
  private long offset;

  @Test
  void putsTheValueAsFieldsOfTheDocumentTheKeyIdentifies() {
    params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
    params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
    SinkRecord record = record("key", "{\"field\":\"value\"}");

    Put put = assertInstanceOf(Put.class, translate(record));

    assertSame(record, put.record);
    assertEquals(DocumentId.of("id:ns:dt::key"), put.documentId);
    assertEquals("{\"fields\":{\"field\":\"value\"}}", put.documentJson);
    assertEquals(DEFAULT_PARAMETERS, put.parameters);
  }

  @Test
  void removesTheDocumentOfTombstone() {
    SinkRecord tombstone = record("key", null);

    Remove remove = assertInstanceOf(Remove.class, translate(tombstone));

    assertSame(tombstone, remove.record);
    assertEquals(DocumentId.of("id:topic:topic::key"), remove.documentId);
    assertEquals(DEFAULT_PARAMETERS, remove.parameters);
  }

  @Test
  void appliesTimeoutRouteAndTracelevel() {
    params.put(VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "20");
    params.put(VespaSinkConfig.OPERATION_ROUTE_CONFIG, "custom");
    params.put(VespaSinkConfig.OPERATION_TRACELEVEL_CONFIG, "3");

    DocumentOperation operation = translate(record("key", "{}"));

    OperationParameters expected =
        OperationParameters.empty().timeout(Duration.ofMillis(20)).route("custom").tracelevel(3);
    assertEquals(expected, operation.parameters);
  }

  @ParameterizedTest
  @MethodSource("emptyValues")
  void rejectsAnEmptyValueAsAnInvalidMessageRatherThanRemoving(Object value) {
    assertInvalidMessage(
        new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", null, value, offset++));
  }

  static Stream<Object> emptyValues() {
    return Stream.of("", new byte[0], " \n\t ", " ".getBytes(StandardCharsets.UTF_8));
  }

  private void assertInvalidMessage(SinkRecord record) {
    VespaFeedException thrown =
        assertThrows(VespaFeedException.class, () -> translator().operations(List.of(record)));
    assertTrue(thrown.getMessage().startsWith("invalid message: "), thrown.getMessage());
  }

  private DocumentOperation translate(SinkRecord record) {
    List<DocumentOperation> operations = translator().operations(List.of(record));
    assertEquals(1, operations.size());
    return operations.get(0);
  }

  private UpsertTranslator translator() {
    return new UpsertTranslator(
        new VespaSinkConfig(params),
        new RecordReporter((record, error) -> CompletableFuture.completedFuture(null)));
  }

  private SinkRecord record(String key, String value) {
    Schema valueSchema = value == null ? null : Schema.STRING_SCHEMA;
    return new SinkRecord(
        "topic", 0, Schema.OPTIONAL_STRING_SCHEMA, key, valueSchema, value, offset++);
  }
}
