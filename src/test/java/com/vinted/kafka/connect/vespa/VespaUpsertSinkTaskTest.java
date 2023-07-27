package com.vinted.kafka.connect.vespa;

import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.mocks.MockVespaFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VespaUpsertSinkTaskTest {
    private long offset = 1;
    private SinkRecord lastRecord;
    private Map<String, String> params;
    private MockVespaFeedClient client;
    private VespaSinkTask task;

    @BeforeEach
    void before() {
        params = new HashMap<>();
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "test_namespace");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "test_document_type");

        client = new MockVespaFeedClient();
        task = new VespaSinkTask();
        task.start(params, client);
    }

    @Test
    void writesDocumentsToVespa() {
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

    @AfterEach
    void after() {
        if (this.task != null) {
            this.task.stop();
        }
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

        return lastRecord = new SinkRecord("topic", 1, keySchema, key, valueSchema, value, offset++);
    }
}
