package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.OperationParameters;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.mocks.FakeFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertNothingSent;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertSent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VespaRawSinkTaskTest {
    private long offset = 1;
    private final Map<String, String> params = new HashMap<>();
    private final FakeFeedClient client = new FakeFeedClient();
    private final ConnectWorker worker = new ConnectWorker(new VespaSinkTask(config -> client));

    @BeforeEach
    void before() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "test_namespace");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "test_document_type");
        params.put(VespaSinkConfig.OPERATIONAL_MODE_CONFIG, VespaSinkConfig.OperationalMode.RAW.name());
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, VespaSinkConfig.BehaviorOnMalformedDoc.WARN.name());
        worker.start(params);
    }

    @AfterEach
    void after() {
        worker.stop();
    }

    @Test
    void writesDocumentsToVespa() {
        List<SinkRecord> records = Collections.singletonList(
                record("set1", "{\"put\":\"id:test_namespace:test_document_type::set1\",\"fields\":{\"field\":\"value1\"}}")
        );

        worker.put(records);

        assertSent(client, "put id:test_namespace:test_document_type::set1 {\"fields\":{\"field\":\"value1\"}}");
    }

    @Test
    void deletesDocumentsFromVespa() {
        List<SinkRecord> records = Collections.singletonList(
                record("del2", "{\"remove\":\"id:test_namespace:test_document_type::del2\"}")
        );

        worker.put(records);

        assertSent(client, "remove id:test_namespace:test_document_type::del2");
    }

    @Test
    void sendsEveryOperationInOrderEvenForTheSameDocument() {
        worker.put(Arrays.asList(
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

        worker.put(Arrays.asList(malformedPut, malformedRemove));

        assertEquals(List.of(malformedPut, malformedRemove), worker.reported());
        assertNothingSent(client);
    }

    @Test
    void skipsTombstones() {
        worker.put(Collections.singletonList(record("set1", null)));

        assertEquals(List.of(), worker.reported());
        assertNothingSent(client);
    }

    @Test
    void skipsTombstonesAmongOperations() {
        List<SinkRecord> records = Arrays.asList(
                record("set1", null),
                record("set2", "{\"put\":\"id:test_namespace:test_document_type::set2\",\"fields\":{\"field\":\"value2\"}}")
        );

        worker.put(records);

        assertEquals(List.of(), worker.reported());
        assertSent(client, "put id:test_namespace:test_document_type::set2 {\"fields\":{\"field\":\"value2\"}}");
    }

    @Test
    void appliesTheConditionOfTheOperationRatherThanTheConfiguredOne() {
        params.put(VespaSinkConfig.OPERATION_CONDITION_CONFIG, "test_document_type.locked == false");
        restart();

        worker.put(Arrays.asList(
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

        worker.put(Arrays.asList(invalid, record("set1", "{\"put\":\"id:test_namespace:test_document_type::set1\",\"fields\":{}}")));

        assertEquals(List.of(invalid), worker.reported());
        assertInstanceOf(DataException.class, worker.reports().get(0).error);
        assertSent(client, "put id:test_namespace:test_document_type::set1 {\"fields\":{}}");
    }

    @Test
    void reportsAndFailsOnInvalidMessagesBeforeSendingAnyOperation() {
        SinkRecord invalid = unconvertible();
        List<SinkRecord> records = Arrays.asList(
                record("set1", "{\"put\":\"id:test_namespace:test_document_type::set1\",\"fields\":{}}"), invalid);

        assertThrows(ConnectException.class, () -> worker.put(records));

        assertEquals(List.of(invalid), worker.reported());
        assertInstanceOf(DataException.class, worker.reports().get(0).error);
        assertNothingSent(client);
    }

    private void restart() {
        worker.restart(params);
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
