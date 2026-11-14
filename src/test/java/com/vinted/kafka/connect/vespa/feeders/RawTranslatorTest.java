package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentJson;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RawTranslatorTest {
    private static final String UPDATE = "{\"update\":\"id:ns:dt::a\",\"fields\":{\"n\":{\"increment\":1}}}";

    private long offset;

    @Test
    void feedsTheValueAsADocumentJsonOperation() {
        SinkRecord record = record(UPDATE);

        List<DocumentJson> operations = translator().operations(List.of(record));

        assertEquals(1, operations.size());
        assertSame(record, operations.get(0).record);
        assertEquals(UPDATE, operations.get(0).json);
    }

    @Test
    void skipsTombstones() {
        assertEquals(List.of(), translator().operations(List.of(record(null))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " \n\t "})
    void rejectsAnEmptyValueAsAnInvalidMessage(String value) {
        assertInvalidMessage(record(value));
    }

    @Test
    void rejectsAValueTheConverterCannotConvertAsAnInvalidMessage() {
        assertInvalidMessage(new SinkRecord("topic", 0, null, null, Schema.STRING_SCHEMA, 42, offset++));
    }

    @Test
    void keepsEveryOperationInOrder() {
        List<SinkRecord> records = List.of(record(UPDATE), record(UPDATE));

        List<DocumentJson> operations = translator().operations(records);

        assertEquals(records, operations.stream().map(operation -> operation.record).collect(Collectors.toList()));
    }

    private void assertInvalidMessage(SinkRecord record) {
        VespaFeedException thrown = assertThrows(VespaFeedException.class, () -> translator().operations(List.of(record)));
        assertTrue(thrown.getMessage().startsWith("invalid message: "), thrown.getMessage());
    }

    private RawTranslator translator() {
        return new RawTranslator(false, (record, error) -> CompletableFuture.completedFuture(null));
    }

    private SinkRecord record(String value) {
        Schema valueSchema = value == null ? null : Schema.STRING_SCHEMA;
        return new SinkRecord("topic", 0, null, null, valueSchema, value, offset++);
    }
}
