package com.vinted.kafka.connect.vespa.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ValueConverterTest {
    private ValueConverter subject;

    @BeforeEach
    void setup() {
        subject = new ValueConverter();
    }

    @Test
    void handlesNullGracefully() {
        SinkRecord record = new SinkRecord("topic", 0, null, "key", null, null, 0);

        assertNull(subject.convert(record));
    }

    @Test
    void convertsStringToPayload() {
        String value = "{\"title\":\"The Lord of the Rings\"}";

        SinkRecord record = new SinkRecord("topic", 0, null, "key", null, value, 0);

        assertEquals("{\"title\":\"The Lord of the Rings\"}", subject.convert(record));
    }

    @Test
    void convertsRecordToPayload() {
        Schema schema = SchemaBuilder
                .struct()
                .field("title", Schema.STRING_SCHEMA)
                .field("description", Schema.STRING_SCHEMA)
                .field("year", Schema.INT32_SCHEMA)
                .build();

        Struct value = new Struct(schema)
                .put("title", "The Lord of the Rings")
                .put("description", "The Lord of the Rings is an epic high-fantasy novel by English author and scholar J. R. R. Tolkien.")
                .put("year", 1954);

        SinkRecord record = new SinkRecord("topic", 0, null, "key", schema, value, 0);

        assertEquals("{\"title\":\"The Lord of the Rings\",\"description\":\"The Lord of the Rings is an epic high-fantasy novel by English author and scholar J. R. R. Tolkien.\",\"year\":1954}", subject.convert(record));
    }
}
