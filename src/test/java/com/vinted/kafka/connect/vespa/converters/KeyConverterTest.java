package com.vinted.kafka.connect.vespa.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KeyConverterTest {
    private KeyConverter subject;

    @BeforeEach
    void setup() {
        subject = new KeyConverter();
    }

    @Test
    void convertsKeyToString() {
        SinkRecord record = new SinkRecord("topic", 0, null, "key", null, null, 0);

        assertEquals("key", subject.convert(record));
    }

    @Test
    void convertsIntegerKeyWithSchemaToString() {
        SinkRecord record = new SinkRecord("topic", 0, Schema.INT32_SCHEMA, 1991, null, null, 0);

        assertEquals("1991", subject.convert(record));
    }

    @Test
    void convertsIntegerKeyWithoutSchemaToString() {
        SinkRecord record = new SinkRecord("topic", 0, null, 1991, null, null, 0);

        assertEquals("1991", subject.convert(record));
    }
}
