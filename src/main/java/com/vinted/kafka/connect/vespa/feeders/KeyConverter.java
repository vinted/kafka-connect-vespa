package com.vinted.kafka.connect.vespa.feeders;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

final class KeyConverter {
  private KeyConverter() {}

  static String convert(SinkRecord record) {
    Schema keySchema = record.keySchema();
    Object key = record.key();

    if (key == null) {
      throw new DataException("Key is used as document id and can not be null.");
    }

    if (String.valueOf(key).isEmpty()) {
      throw new DataException("Key is used as document id and can not be empty.");
    }

    Schema.Type schemaType;

    if (keySchema == null) {
      schemaType = ConnectSchema.schemaType(key.getClass());
    } else {
      schemaType = keySchema.type();
    }

    if (schemaType == null) {
      throw new DataException(
          "Java class " + key.getClass() + " does not have corresponding schema type.");
    }

    switch (schemaType) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        return String.valueOf(key instanceof java.util.Date ? epochNumber(keySchema, key) : key);
      case STRING:
        return String.valueOf(key);
      default:
        throw new DataException(schemaType.name() + " is not supported as the document id.");
    }
  }

  // Days for a date, milliseconds for a time or timestamp, as for values, rather than a string that
  // depends on the worker's time zone.
  private static long epochNumber(Schema schema, Object key) {
    java.util.Date date = (java.util.Date) key;
    if (Date.LOGICAL_NAME.equals(schema.name())) {
      return Date.fromLogical(schema, date);
    }
    if (Time.LOGICAL_NAME.equals(schema.name())) {
      return Time.fromLogical(schema, date);
    }
    return Timestamp.fromLogical(schema, date);
  }
}
