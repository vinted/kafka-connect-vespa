package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentJson;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RawTranslatorTest {
    private static final String UPDATE = "{\"update\":\"id:ns:dt::a\",\"fields\":{\"n\":{\"increment\":1}}}";

    private long offset;

    @Test
    void feedsTheValueAsADocumentJsonOperation() {
        SinkRecord record = record(UPDATE);

        DocumentJson operation = translator().translate(record).orElseThrow();

        assertSame(record, operation.record);
        assertEquals(UPDATE, operation.json);
    }

    @Test
    void skipsTombstones() {
        assertEquals(Optional.empty(), translator().translate(record(null)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " \n\t "})
    void rejectsAnEmptyValueAsAnInvalidMessage(String value) {
        assertThrows(DataException.class, () -> translator().translate(record(value)));
    }

    @Test
    void rejectsAValueTheConverterCannotConvertAsAnInvalidMessage() {
        SinkRecord record = new SinkRecord("topic", 0, null, null, Schema.STRING_SCHEMA, 42, offset++);

        assertThrows(DataException.class, () -> translator().translate(record));
    }

    @Test
    void keepsEveryOperationInOrder() {
        RawTranslator translator = translator();
        List<DocumentJson> operations = List.of(
                translator.translate(record(UPDATE)).orElseThrow(),
                translator.translate(record(UPDATE)).orElseThrow());

        assertEquals(operations, translator.compact(operations));
    }

    private RawTranslator translator() {
        return new RawTranslator();
    }

    private SinkRecord record(String value) {
        Schema valueSchema = value == null ? null : Schema.STRING_SCHEMA;
        return new SinkRecord("topic", 0, null, null, valueSchema, value, offset++);
    }
}
