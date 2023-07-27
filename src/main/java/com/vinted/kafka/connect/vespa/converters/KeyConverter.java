package com.vinted.kafka.connect.vespa.converters;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class KeyConverter {
    public String convert(SinkRecord record) {
        Schema keySchema = record.keySchema();
        Object key = record.key();

        if (key == null) {
            throw new DataException("Key is used as document id and can not be null.");
        }

        if (String.valueOf(key).isBlank()) {
            throw new DataException("Key is used as document id and can not be empty.");
        }

        Schema.Type schemaType;

        if (keySchema == null) {
            schemaType = ConnectSchema.schemaType(key.getClass());
        } else {
            schemaType = keySchema.type();
        }

        if (schemaType == null) {
            throw new DataException("Java class " + key.getClass() + " does not have corresponding schema type.");
        }

        switch (schemaType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case STRING:
                return String.valueOf(key);
            default:
                throw new DataException(schemaType.name() + " is not supported as the document id.");
        }
    }
}
