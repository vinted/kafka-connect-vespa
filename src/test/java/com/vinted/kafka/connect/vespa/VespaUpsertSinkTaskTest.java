package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.MultiFeedException;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertNothingSent;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertSent;
import static com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient.Answer.result;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VespaUpsertSinkTaskTest {
    private long offset = 1;
    private final Map<String, String> params = new HashMap<>();
    private final RecordingFeedClient client = new RecordingFeedClient();
    private final VespaSinkTask task = new VespaSinkTask(config -> client);
    private final String topic = UUID.randomUUID().toString();
    private final SinkTaskContext context = mock(SinkTaskContext.class);
    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    @BeforeEach
    void before() {
        when(context.errantRecordReporter()).thenReturn(reporter);
        task.initialize(context);
    }

    @AfterEach
    void after() {
        this.task.stop();
    }

    @Test
    void writesDocumentsToVespa() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "test_namespace");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "test_document_type");

        task.start(params);

        task.put(Arrays.asList(
                record("set1", "{\"field\":\"value1\"}"),
                record("set2", "{\"field\":\"value2\"}"),
                record("delete1", null)
        ));

        assertSent(client,
                "put id:test_namespace:test_document_type::set1 {\"fields\":{\"field\":\"value1\"}}",
                "put id:test_namespace:test_document_type::set2 {\"fields\":{\"field\":\"value2\"}}",
                "remove id:test_namespace:test_document_type::delete1");
    }

    @Test
    void appliesConditionToPutsAndRemovesButCreateOnlyToPuts() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
        params.put(VespaSinkConfig.OPERATION_CONDITION_CONFIG, "dt.locked == false");
        params.put(VespaSinkConfig.OPERATION_CREATE_IF_NON_EXISTENT_CONFIG, "true");

        task.start(params);
        task.put(Arrays.asList(record("set1", "{\"field\":\"value1\"}"), record("delete1", null)));

        assertSent(client, "put id:ns:dt::set1 {\"fields\":{\"field\":\"value1\"}}", "remove id:ns:dt::delete1");

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

        task.start(params);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                assertThrows(ConnectException.class, () -> task.put(List.of(record("set1", "{\"field\":\"value1\"}")))));
    }

    @Test
    void keepsTheCauseOfRejectedResults() {
        client.answerAll(result(Result.Type.conditionNotMet));

        task.start(params);

        ConnectException thrown = assertThrows(ConnectException.class,
                () -> task.put(List.of(record("set1", "{\"field\":\"value1\"}"))));
        MultiFeedException cause = assertInstanceOf(MultiFeedException.class, thrown.getCause());
        assertEquals(1, cause.feedExceptions().size());
    }

    @Test
    void failsInsteadOfHangingWhenErrantRecordReporterThrows() {
        client.answerAll(result(Result.Type.conditionNotMet));
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
        when(reporter.report(any(), any())).thenThrow(new ConnectException("Tolerance exceeded in error handler"));

        task.start(params);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                assertThrows(ConnectException.class, () -> task.put(List.of(record("set1", "{\"field\":\"value1\"}")))));
    }

    @Test
    void ignoresConditionNotMetWhenConfigured() {
        client.answerAll(result(Result.Type.conditionNotMet));
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");

        task.start(params);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                task.put(List.of(record("set1", "{\"field\":\"value1\"}"))));
    }

    @Test
    void reportsAndDropsInvalidMessagesWhenConfigured() {
        params.put(VespaSinkConfig.DROP_INVALID_MESSAGE_CONFIG, "true");
        SinkRecord invalid = record(null, "{\"field\":\"value1\"}");

        task.start(params);
        task.put(Arrays.asList(invalid, record("set1", "{\"field\":\"value1\"}")));

        verify(reporter).report(eq(invalid), any(DataException.class));
        assertSent(client, String.format("put id:%s:%s::set1 {\"fields\":{\"field\":\"value1\"}}", topic, topic));
    }

    @Test
    void reportsAndFailsOnInvalidMessagesByDefault() {
        SinkRecord invalid = record(null, "{\"field\":\"value1\"}");

        task.start(params);

        assertThrows(ConnectException.class, () -> task.put(List.of(invalid)));
        verify(reporter).report(eq(invalid), any(DataException.class));
        assertNothingSent(client);
    }

    @Test
    void reportsRejectedResults() {
        client.answerAll(result(Result.Type.conditionNotMet));
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
        SinkRecord rejected = record("set1", "{\"field\":\"value1\"}");

        task.start(params);
        task.put(List.of(rejected));

        verify(reporter).report(eq(rejected), any());
    }

    @Test
    void doesNotReportSuccessfulRecords() {
        task.start(params);
        task.put(List.of(record("set1", "{\"field\":\"value1\"}")));

        verify(reporter, never()).report(any(), any());
    }

    @Test
    void feedsWithoutErrantRecordReporter() {
        when(context.errantRecordReporter()).thenReturn(null);

        task.start(params);
        task.put(List.of(record("set1", "{\"field\":\"value1\"}")));

        assertSent(client, String.format("put id:%s:%s::set1 {\"fields\":{\"field\":\"value1\"}}", topic, topic));
    }

    @Test
    void startsOnRuntimesWithoutErrantRecordReporter() {
        doThrow(new NoSuchMethodError()).when(context).errantRecordReporter();
        task.start(params);

        doThrow(new NoClassDefFoundError()).when(context).errantRecordReporter();
        task.start(params);
    }

    @Test
    void writesDocumentsWithTopicNameToVespa() {
        task.start(params);

        task.put(Arrays.asList(record("set1", "{\"field\":\"value1\"}"), record("delete1", null)));

        assertSent(client,
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
}
