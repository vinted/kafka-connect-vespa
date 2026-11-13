package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.mocks.MockVespaFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VespaUpsertSinkTaskTest {
    private long offset = 1;
    private final Map<String, String> params = new HashMap<>();
    private final MockVespaFeedClient client = new MockVespaFeedClient();
    private final VespaSinkTask task = new VespaSinkTask();
    private final String topic = UUID.randomUUID().toString();

    @AfterEach
    void after() {
        this.task.stop();
    }

    @Test
    void writesDocumentsToVespa() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "test_namespace");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "test_document_type");

        task.start(params, client);

        List<SinkRecord> records = Arrays.asList(
                record("set1", "{\"field\":\"value1\"}"),
                record("set2", "{\"field\":\"value2\"}"),
                record("delete1", null),
                record("set3", "{\"field\":\"value3\"}"),
                record("set4", "{\"field\":\"value4_old\"}"),
                record("set4", "{\"field\":\"value4\"}")
        );

        task.put(records);

        client.assertAllDocumentIds(
                "id:test_namespace:test_document_type::set1",
                "id:test_namespace:test_document_type::delete1",
                "id:test_namespace:test_document_type::set2",
                "id:test_namespace:test_document_type::set3",
                "id:test_namespace:test_document_type::set4"
        );

        client.assertPutOperation(
                "id:test_namespace:test_document_type::set1",
                "{\"fields\":{\"field\":\"value1\"}}"
        );

        client.assertPutOperation(
                "id:test_namespace:test_document_type::set2",
                "{\"fields\":{\"field\":\"value2\"}}"
        );

        client.assertPutOperation(
                "id:test_namespace:test_document_type::set3",
                "{\"fields\":{\"field\":\"value3\"}}"
        );

        client.assertPutOperation(
                "id:test_namespace:test_document_type::set4",
                "{\"fields\":{\"field\":\"value4\"}}"
        );
    }

    @Test
    void appliesConditionToPutsAndRemovesButCreateOnlyToPuts() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
        params.put(VespaSinkConfig.OPERATION_CONDITION_CONFIG, "dt.locked == false");
        params.put(VespaSinkConfig.OPERATION_CREATE_IF_NON_EXISTENT_CONFIG, "true");

        task.start(params, client);
        task.put(Arrays.asList(record("set1", "{\"field\":\"value1\"}"), record("delete1", null)));

        OperationParameters put = client.parameters.get(DocumentId.of("id:ns:dt::set1"));
        assertEquals(Optional.of("dt.locked == false"), put.testAndSetCondition());
        assertTrue(put.createIfNonExistent());

        OperationParameters remove = client.parameters.get(DocumentId.of("id:ns:dt::delete1"));
        assertEquals(Optional.of("dt.locked == false"), remove.testAndSetCondition());
        assertFalse(remove.createIfNonExistent());
    }

    @Test
    void failsInsteadOfHangingWhenConditionIsNotMet() {
        client.resultType = Result.Type.conditionNotMet;

        task.start(params, client);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                assertThrows(ConnectException.class, () -> task.put(List.of(record("set1", "{\"field\":\"value1\"}")))));
    }

    @Test
    void ignoresConditionNotMetWhenConfigured() {
        client.resultType = Result.Type.conditionNotMet;
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");

        task.start(params, client);

        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                task.put(List.of(record("set1", "{\"field\":\"value1\"}"))));
    }

    @Test
    void writesDocumentsWithTopicNameToVespa() {
        task.start(params, client);

        List<SinkRecord> records = Arrays.asList(
                record("set1", "{\"field\":\"value1\"}"),
                record("set2", "{\"field\":\"value2\"}"),
                record("delete1", null),
                record("set3", "{\"field\":\"value3\"}"),
                record("set4", "{\"field\":\"value4_old\"}"),
                record("set4", "{\"field\":\"value4\"}")
        );

        task.put(records);

        client.assertAllDocumentIds(
                String.format("id:%s:%s::set1", topic, topic),
                String.format("id:%s:%s::delete1", topic, topic),
                String.format("id:%s:%s::set2", topic, topic),
                String.format("id:%s:%s::set3", topic, topic),
                String.format("id:%s:%s::set4", topic, topic)
        );
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
