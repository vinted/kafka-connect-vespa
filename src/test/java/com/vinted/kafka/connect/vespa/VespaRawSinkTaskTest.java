package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.OperationParameters;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VespaRawSinkTaskTest {
    private long offset = 1;
    private final Map<String, String> params = new HashMap<>();
    private final RecordingFeedClient client = new RecordingFeedClient();
    private final VespaSinkTask task = new VespaSinkTask(config -> client);
    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    @BeforeEach
    void before() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "test_namespace");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "test_document_type");
        params.put(VespaSinkConfig.OPERATIONAL_MODE_CONFIG, VespaSinkConfig.OperationalMode.RAW.name());
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, VespaSinkConfig.BehaviorOnMalformedDoc.WARN.name());
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.errantRecordReporter()).thenReturn(reporter);
        task.initialize(context);
        task.start(params);
    }

    @AfterEach
    void after() {
        this.task.stop();
    }

    @Test
    void writesDocumentsToVespa() {
        List<SinkRecord> records = Collections.singletonList(
                record("set1", "{\"put\":\"id:test_namespace:test_document_type::set1\",\"fields\":{\"field\":\"value1\"}}")
        );

        task.put(records);

        assertSent(client, "put id:test_namespace:test_document_type::set1 {\"fields\":{\"field\":\"value1\"}}");
    }

    @Test
    void deletesDocumentsFromVespa() {
        List<SinkRecord> records = Collections.singletonList(
                record("del2", "{\"remove\":\"id:test_namespace:test_document_type::del2\"}")
        );

        task.put(records);

        assertSent(client, "remove id:test_namespace:test_document_type::del2");
    }

    @Test
    void sendsEveryOperationInOrderEvenForTheSameDocument() {
        task.put(Arrays.asList(
                record("doc", "{\"put\":\"id:test_namespace:test_document_type::doc\",\"fields\":{\"n\":1}}"),
                record("doc", "{\"update\":\"id:test_namespace:test_document_type::doc\",\"fields\":{\"n\":{\"increment\":1}}}"),
                record("doc", "{\"remove\":\"id:test_namespace:test_document_type::doc\"}"),
                record("doc", "{\"put\":\"id:test_namespace:test_document_type::doc\",\"fields\":{\"n\":3}}")
        ));

        assertSent(client,
                "put id:test_namespace:test_document_type::doc {\"fields\":{\"n\":1}}",
                "update id:test_namespace:test_document_type::doc {\"fields\":{\"n\":{\"increment\":1}}}",
                "remove id:test_namespace:test_document_type::doc",
                "put id:test_namespace:test_document_type::doc {\"fields\":{\"n\":3}}");
    }

    @Test
    void reportsAndSkipsMalformedPayloads() {
        SinkRecord malformedPut = record("set1", "{put\":\"id:test_namespace:test_document_type::set1\"}");
        SinkRecord malformedRemove = record("del2", "{remove\":\"id:test_namespace:test_document_type::del2\"}");

        task.put(Arrays.asList(malformedPut, malformedRemove));

        verify(reporter).report(eq(malformedPut), any());
        verify(reporter).report(eq(malformedRemove), any());
        assertNothingSent(client);
    }

    @Test
    void skipsTombstones() {
        task.put(Collections.singletonList(record("set1", null)));

        verify(reporter, never()).report(any(), any());
        assertNothingSent(client);
    }

    @Test
    void skipsTombstonesAmongOperations() {
        List<SinkRecord> records = Arrays.asList(
                record("set1", null),
                record("set2", "{\"put\":\"id:test_namespace:test_document_type::set2\",\"fields\":{\"field\":\"value2\"}}")
        );

        task.put(records);

        verify(reporter, never()).report(any(), any());
        assertSent(client, "put id:test_namespace:test_document_type::set2 {\"fields\":{\"field\":\"value2\"}}");
    }

    @Test
    void appliesTheConditionOfTheOperationRatherThanTheConfiguredOne() {
        params.put(VespaSinkConfig.OPERATION_CONDITION_CONFIG, "test_document_type.locked == false");
        restart();

        task.put(Arrays.asList(
                record("set1", "{\"put\":\"id:test_namespace:test_document_type::set1\",\"fields\":{}}"),
                record("set2", "{\"put\":\"id:test_namespace:test_document_type::set2\",\"condition\":\"test_document_type.n > 1\",\"fields\":{}}")
        ));

        assertSent(client,
                "put id:test_namespace:test_document_type::set1 {\"fields\":{}}",
                "put id:test_namespace:test_document_type::set2 {\"fields\":{}}");
        OperationParameters unconditional = client.operations().get(0).parameters;
        OperationParameters conditional = client.operations().get(1).parameters;
        assertEquals(Optional.empty(), unconditional.testAndSetCondition());
        assertEquals(Optional.of("test_document_type.n > 1"), conditional.testAndSetCondition());
        assertEquals(Optional.of(Duration.ofMinutes(1)), conditional.timeout());
    }

    @Test
    void reportsAndDropsInvalidMessagesWhenConfigured() {
        params.put(VespaSinkConfig.DROP_INVALID_MESSAGE_CONFIG, "true");
        restart();
        SinkRecord invalid = unconvertible();

        task.put(Arrays.asList(invalid, record("set1", "{\"put\":\"id:test_namespace:test_document_type::set1\",\"fields\":{}}")));

        verify(reporter).report(eq(invalid), any(DataException.class));
        assertSent(client, "put id:test_namespace:test_document_type::set1 {\"fields\":{}}");
    }

    @Test
    void reportsAndFailsOnInvalidMessagesBeforeSendingAnyOperation() {
        SinkRecord invalid = unconvertible();
        List<SinkRecord> records = Arrays.asList(
                record("set1", "{\"put\":\"id:test_namespace:test_document_type::set1\",\"fields\":{}}"), invalid);

        assertThrows(ConnectException.class, () -> task.put(records));

        verify(reporter).report(eq(invalid), any(DataException.class));
        assertNothingSent(client);
    }

    private void restart() {
        task.stop();
        task.start(params);
    }

    private SinkRecord unconvertible() {
        return new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, 42, offset++);
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

        return new SinkRecord("topic", 1, keySchema, key, valueSchema, value, offset++);
    }
}
