package com.vinted.kafka.connect.vespa.converters;

import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ValueConverter {
    private static final Converter JSON_CONVERTER;

    static {
        JSON_CONVERTER = new JsonConverter();
        // Vespa cannot read the default base64 decimals.
        JSON_CONVERTER.configure(Map.of(
                JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false",
                JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()), false);
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
