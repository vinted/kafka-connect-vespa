package com.vinted.kafka.connect.vespa.converters;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class ValueConverter {
    private static final Converter JSON_CONVERTER;

    static {
        JSON_CONVERTER = new JsonConverter();
        JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public String convert(SinkRecord record) {
        if (record.value() == null) {
            return null;
        } else if (record.value() instanceof byte[]) {
            return new String((byte[]) record.value(), StandardCharsets.UTF_8);
        } else if (record.value() instanceof String) {
            return (String) record.value();
        } else {
            byte[] bytes = JSON_CONVERTER.fromConnectData(record.topic(), record.valueSchema(), record.value());
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
