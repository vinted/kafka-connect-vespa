package com.vinted.kafka.connect.vespa;

import static com.vinted.kafka.connect.vespa.mocks.FakeFeedClient.Answer.failure;
import static com.vinted.kafka.connect.vespa.mocks.FakeFeedClient.Answer.refusal;
import static com.vinted.kafka.connect.vespa.mocks.FakeFeedClient.Answer.result;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertNothingSent;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertSent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException;
import com.vinted.kafka.connect.vespa.mocks.CallbackExecutor;
import com.vinted.kafka.connect.vespa.mocks.FakeFeedClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class VespaUpsertSinkTaskTest {
  private long offset = 1;
  private final Map<String, String> params = new HashMap<>();
  private FakeFeedClient client = new FakeFeedClient();
  private final ConnectWorker worker = new ConnectWorker(new VespaSinkTask(config -> client));
  private final String topic = UUID.randomUUID().toString();

  @AfterEach
  void after() {
    worker.stop();
  }

  @Test
  void writesDocumentsToVespa() {
    params.put(VespaSinkConfig.NAMESPACE_CONFIG, "test_namespace");
    params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "test_document_type");

    worker.start(params);

    worker.put(
        Arrays.asList(
            record("set1", "{\"field\":\"value1\"}"),
            record("set2", "{\"field\":\"value2\"}"),
            record("delete1", null)));

    assertSent(
        client,
        "put id:test_namespace:test_document_type::set1 {\"fields\":{\"field\":\"value1\"}}",
        "put id:test_namespace:test_document_type::set2 {\"fields\":{\"field\":\"value2\"}}",
        "remove id:test_namespace:test_document_type::delete1");
  }

  @Test
  void sendsOnlyTheLastOperationPerDocumentInOrderOfFirstAppearance() {
    params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
    params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");

    worker.start(params);

    worker.put(
        Arrays.asList(
            record("a", "{\"v\":1}"),
            record("b", "{\"v\":1}"),
            record("a", "{\"v\":2}"),
            record("c", null),
            record("b", null),
            record("c", "{\"v\":2}")));

    assertSent(
        client,
        "put id:ns:dt::a {\"fields\":{\"v\":2}}",
        "remove id:ns:dt::b",
        "put id:ns:dt::c {\"fields\":{\"v\":2}}");
  }

  @Test
  void appliesConditionToPutsAndRemovesButCreateOnlyToPuts() {
    params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
    params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
    params.put(VespaSinkConfig.OPERATION_CONDITION_CONFIG, "dt.locked == false");
    params.put(VespaSinkConfig.OPERATION_CREATE_IF_NON_EXISTENT_CONFIG, "true");

    worker.start(params);
    worker.put(Arrays.asList(record("set1", "{\"field\":\"value1\"}"), record("delete1", null)));

    assertSent(
        client,
        "put id:ns:dt::set1 {\"fields\":{\"field\":\"value1\"}}",
        "remove id:ns:dt::delete1");

    OperationParameters put = client.operations().get(0).parameters;
    assertEquals(Optional.of("dt.locked == false"), put.testAndSetCondition());
    assertTrue(put.createIfNonExistent());

    OperationParameters remove = client.operations().get(1).parameters;
    assertEquals(Optional.of("dt.locked == false"), remove.testAndSetCondition());
    assertFalse(remove.createIfNonExistent());
  }

  @Test
  void failsInsteadOfHangingWhenConditionIsNotMet() {
    client.answerAll(result(Result.Type.conditionNotMet));

    worker.start(params);

    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () ->
            assertThrows(
                ConnectException.class,
                () -> worker.put(List.of(record("set1", "{\"field\":\"value1\"}")))));
  }

  @Test
  void keepsTheCauseOfRejectedResults() {
    client.answerAll(result(Result.Type.conditionNotMet));

    worker.start(params);

    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () -> worker.put(List.of(record("set1", "{\"field\":\"value1\"}"))));
    assertInstanceOf(FeedException.class, thrown.getCause());
    assertTrue(
        thrown.getMessage().startsWith("rejected result in 1 operation"), thrown.getMessage());
  }

  @Test
  void failsInsteadOfHangingWhenErrantRecordReporterThrows() {
    client.answerAll(result(Result.Type.conditionNotMet));
    params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
    worker.failingReports(new ConnectException("Tolerance exceeded in error handler"));

    worker.start(params);
    SinkRecord rejected = record("set1", "{\"field\":\"value1\"}");

    VespaFeedException thrown =
        assertTimeoutPreemptively(
            Duration.ofSeconds(10),
            () -> assertThrows(VespaFeedException.class, () -> worker.put(List.of(rejected))));
    assertFatalFailureToReport(rejected, "FeedException", thrown);
  }

  @ParameterizedTest(name = "drop invalid message: {0}")
  @ValueSource(booleans = {true, false})
  void failsTheTaskWhenErrantRecordReporterThrowsOverInvalidMessage(boolean dropInvalidMessage) {
    params.put(VespaSinkConfig.DROP_INVALID_MESSAGE_CONFIG, String.valueOf(dropInvalidMessage));
    worker.failingReports(new ConnectException("Tolerance exceeded in error handler"));
    SinkRecord invalid = record(null, "{\"field\":\"value1\"}");

    worker.start(params);

    try (CapturedLogs logs = CapturedLogs.capture()) {
      VespaFeedException thrown =
          assertThrows(
              VespaFeedException.class,
              () -> worker.put(List.of(invalid, record("set1", "{\"field\":\"value1\"}"))));

      assertFatalFailureToReport(invalid, "DataException", thrown);
      assertTrue(
          logs.errors().stream().anyMatch(error -> error.contains("Failing the task")),
          String.join("\n", logs.errors()));
    }
    assertEquals(0, worker.redeliveries());
    assertNothingSent(client);
  }

  @Test
  void reportsAndSkipsRejectedResultsUnderIgnore() {
    params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
    params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
    params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
    client.answer("id:ns:dt::rejected", result(Result.Type.conditionNotMet));
    SinkRecord rejected = record("rejected", "{\"field\":\"value1\"}");

    worker.start(params);
    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> worker.put(List.of(rejected, record("accepted", "{\"field\":\"value2\"}"))));

    assertEquals(List.of(rejected), worker.reported());
    FeedException cause = assertInstanceOf(FeedException.class, worker.reports().get(0).error);
    assertEquals(Optional.of(DocumentId.of("id:ns:dt::rejected")), cause.documentId());
  }

  @Test
  void reportsMalformedDocumentFromCallbackThreadBeforePutReturns() throws Exception {
    params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
    params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
    params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
    try (CallbackExecutor callbacks = new CallbackExecutor(3)) {
      client =
          new FakeFeedClient(callbacks)
              .answer(
                  "id:ns:dt::malformed",
                  failure(
                      new FeedException("Status 400 executing 'POST /document/v1/': bad request")));
      worker.start(params);

      worker.put(List.of(record("before", "{}"), record("malformed", "{}"), record("after", "{}")));

      assertEquals(
          List.of("malformed on " + CallbackExecutor.THREAD_NAME), worker.reportedKeysAndThreads());
    }
  }

  @Test
  void reportsAndDropsInvalidMessagesWhenConfigured() {
    params.put(VespaSinkConfig.DROP_INVALID_MESSAGE_CONFIG, "true");
    SinkRecord invalid = record(null, "{\"field\":\"value1\"}");

    worker.start(params);
    worker.put(Arrays.asList(invalid, record("set1", "{\"field\":\"value1\"}")));

    assertEquals(List.of(invalid), worker.reported());
    assertInstanceOf(DataException.class, worker.reports().get(0).error);
    assertSent(
        client,
        String.format("put id:%s:%s::set1 {\"fields\":{\"field\":\"value1\"}}", topic, topic));
  }

  @Test
  void reportsAndFailsOnInvalidMessagesByDefault() {
    SinkRecord invalid = record(null, "{\"field\":\"value1\"}");

    worker.start(params);

    assertThrows(ConnectException.class, () -> worker.put(List.of(invalid)));
    assertEquals(List.of(invalid), worker.reported());
    assertInstanceOf(DataException.class, worker.reports().get(0).error);
    assertNothingSent(client);
  }

  @Test
  void failsEveryOperationTheClientRefusesInsteadOfLosingTheOthers() {
    IllegalStateException closed = new IllegalStateException("Client is closed");
    client.answerAll(refusal(closed));
    worker.start(params);

    VespaFeedException thrown =
        assertThrows(
            VespaFeedException.class,
            () -> worker.put(List.of(record("set1", "{}"), record("set2", "{}"))));

    assertSame(closed, thrown.getCause());
    assertEquals(
        "fatal failure in 2 operations: IllegalStateException: Client is closed",
        thrown.getMessage());
  }

  @Test
  void closesTheFeedClientWhenStopped() {
    worker.start(params);

    worker.stop();

    assertTrue(client.isClosed());
  }

  @Test
  void doesNotReportSuccessfulRecords() {
    worker.start(params);
    worker.put(List.of(record("set1", "{\"field\":\"value1\"}")));

    assertEquals(List.of(), worker.reported());
  }

  @Test
  void feedsWithoutErrantRecordReporter() {
    worker.withoutReporter();

    worker.start(params);
    worker.put(List.of(record("set1", "{\"field\":\"value1\"}")));

    assertSent(
        client,
        String.format("put id:%s:%s::set1 {\"fields\":{\"field\":\"value1\"}}", topic, topic));
  }

  @Test
  void startsOnRuntimesWithoutErrantRecordReporter() {
    worker.withoutReporterSupport(new NoSuchMethodError()).start(params);
    worker.put(List.of(record("set1", "{\"field\":\"value1\"}")));

    worker.withoutReporterSupport(new NoClassDefFoundError()).restart(params);
    worker.put(List.of(record("set2", "{\"field\":\"value2\"}")));

    assertEquals(2, client.operations().size());
  }

  @Test
  void writesDocumentsWithTopicNameToVespa() {
    worker.start(params);

    worker.put(Arrays.asList(record("set1", "{\"field\":\"value1\"}"), record("delete1", null)));

    assertSent(
        client,
        String.format("put id:%s:%s::set1 {\"fields\":{\"field\":\"value1\"}}", topic, topic),
        String.format("remove id:%s:%s::delete1", topic, topic));
  }

  private SinkRecord record(String key, String value) {
    final Schema keySchema = Schema.STRING_SCHEMA;
    final Schema valueSchema;

    if (Strings.isNullOrEmpty(value)) {
      value = null;
      valueSchema = null;
    } else {
      valueSchema = Schema.STRING_SCHEMA;
    }

    return new SinkRecord(topic, 1, keySchema, key, valueSchema, value, offset++);
  }

  private static void assertFatalFailureToReport(
      SinkRecord record, String failure, VespaFeedException thrown) {
    String message = thrown.getMessage();
    assertFalse(thrown.isTransient(), message);
    assertTrue(message.startsWith("fatal failure"), message);
    assertTrue(message.contains("Failed to report record from topic="), message);
    assertTrue(message.contains("offset=" + record.kafkaOffset()), message);
    assertTrue(message.contains("which failed with " + failure), message);
    assertTrue(message.contains("Tolerance exceeded in error handler"), message);
  }
}
