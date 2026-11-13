package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParameters;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentOperation;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Put;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Remove;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UpsertTranslatorTest {
    private static final OperationParameters DEFAULT_PARAMETERS = OperationParameters.empty().timeout(Duration.ofMinutes(1));

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
    void defaultsTheNamespaceAndDocumentTypeToTheTopic() {
        DocumentOperation operation = translate(record("key", "{}"));

        assertEquals(DocumentId.of("id:topic:topic::key"), operation.documentId);
    }

    @Test
    void removesTheDocumentOfATombstone() {
        SinkRecord tombstone = record("key", null);

        Remove remove = assertInstanceOf(Remove.class, translate(tombstone));

        assertSame(tombstone, remove.record);
        assertEquals(DocumentId.of("id:topic:topic::key"), remove.documentId);
        assertEquals(DEFAULT_PARAMETERS, remove.parameters);
    }

    @Test
    void appliesTheConditionToPutsAndRemovesButCreateOnlyToPuts() {
        params.put(VespaSinkConfig.OPERATION_CONDITION_CONFIG, "dt.locked == false");
        params.put(VespaSinkConfig.OPERATION_CREATE_IF_NON_EXISTENT_CONFIG, "true");

        DocumentOperation put = translate(record("put", "{}"));
        DocumentOperation remove = translate(record("remove", null));

        OperationParameters conditional = DEFAULT_PARAMETERS.testAndSetCondition("dt.locked == false");
        assertEquals(conditional.createIfNonExistent(true), put.parameters);
        assertEquals(conditional, remove.parameters);
    }

    @Test
    void appliesTimeoutRouteAndTracelevel() {
        params.put(VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "20");
        params.put(VespaSinkConfig.OPERATION_ROUTE_CONFIG, "custom");
        params.put(VespaSinkConfig.OPERATION_TRACELEVEL_CONFIG, "3");

        DocumentOperation operation = translate(record("key", "{}"));

        OperationParameters expected = OperationParameters.empty()
                .timeout(Duration.ofMillis(20))
                .route("custom")
                .tracelevel(3);
        assertEquals(expected, operation.parameters);
    }

    @Test
    void rejectsARecordWithoutKeyAsAnInvalidMessage() {
        assertThrows(DataException.class, () -> translator().translate(record(null, "{}")));
    }

    @ParameterizedTest
    @MethodSource("emptyValues")
    void rejectsAnEmptyValueAsAnInvalidMessageRatherThanRemoving(Object value) {
        SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", null, value, offset++);

        assertThrows(DataException.class, () -> translator().translate(record));
    }

    static Stream<Object> emptyValues() {
        return Stream.of("", new byte[0], " \n\t ", " ".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void keepsOnlyTheLastOperationPerDocument() {
        UpsertTranslator translator = translator();
        DocumentOperation first = translator.translate(record("a", "{\"v\":1}")).orElseThrow();
        DocumentOperation other = translator.translate(record("b", "{}")).orElseThrow();
        DocumentOperation removed = translator.translate(record("a", null)).orElseThrow();
        DocumentOperation last = translator.translate(record("a", "{\"v\":2}")).orElseThrow();

        assertEquals(List.of(last, other), translator.compact(List.of(first, other, removed, last)));
    }

    private DocumentOperation translate(SinkRecord record) {
        return translator().translate(record).orElseThrow();
    }

    private UpsertTranslator translator() {
        return new UpsertTranslator(new VespaSinkConfig(params));
    }

    private SinkRecord record(String key, String value) {
        Schema valueSchema = value == null ? null : Schema.STRING_SCHEMA;
        return new SinkRecord("topic", 0, Schema.OPTIONAL_STRING_SCHEMA, key, valueSchema, value, offset++);
    }
}
