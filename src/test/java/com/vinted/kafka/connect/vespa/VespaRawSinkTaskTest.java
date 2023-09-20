package com.vinted.kafka.connect.vespa;

import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.mocks.MockVespaFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class VespaRawSinkTaskTest {
    private long offset = 1;
    private final Map<String, String> params = new HashMap<>();
    private final MockVespaFeedClient client = new MockVespaFeedClient();
    private final VespaSinkTask task = new VespaSinkTask();

    @BeforeEach
    void before() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "test_namespace");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "test_document_type");
        params.put(VespaSinkConfig.OPERATIONAL_MODE_CONFIG, VespaSinkConfig.OperationalMode.RAW.name());
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, VespaSinkConfig.BehaviorOnMalformedDoc.WARN.name());
        task.start(params, client);
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

        client.assertAllDocumentIds(
                "id:test_namespace:test_document_type::set1"
        );

        client.assertPutOperation(
                "id:test_namespace:test_document_type::set1",
                "{\"fields\":{\"field\":\"value1\"}}"
        );
    }

    @Test
    void deletesDocumentsFromVespa() {
        List<SinkRecord> records = Collections.singletonList(
                record("del2", "{\"remove\":\"id:test_namespace:test_document_type::del2\"}")
        );

        task.put(records);

        client.assertAllDocumentIds(
                "id:test_namespace:test_document_type::del2"
        );
    }

    @Test
    void handlesMalformedPayloads() {
        List<SinkRecord> records = Arrays.asList(
                record("set1", "{put\":\"id:test_namespace:test_document_type::set1\"}"),
                record("del2", "{remove\":\"id:test_namespace:test_document_type::del2\"}")
        );

        task.put(records);
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
