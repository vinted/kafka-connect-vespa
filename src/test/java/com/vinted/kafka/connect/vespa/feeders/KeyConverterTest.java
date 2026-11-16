package com.vinted.kafka.connect.vespa.feeders;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class KeyConverterTest {
  @Test
  void convertsKeyToString() {
    SinkRecord record = new SinkRecord("topic", 0, null, "key", null, null, 0);

    assertEquals("key", KeyConverter.convert(record));
  }

  @Test
  void convertsIntegerKeyWithSchemaToString() {
    SinkRecord record = new SinkRecord("topic", 0, Schema.INT32_SCHEMA, 1991, null, null, 0);

    assertEquals("1991", KeyConverter.convert(record));
  }

  @Test
  void convertsDateKeyToDaysSinceTheEpoch() {
    SinkRecord record =
        new SinkRecord("topic", 0, Date.SCHEMA, Date.toLogical(Date.SCHEMA, 20000), null, null, 0);

    assertEquals("20000", KeyConverter.convert(record));
  }

  @Test
  void convertsTimeKeyToMillisecondsSinceMidnight() {
    SinkRecord record =
        new SinkRecord(
            "topic", 0, Time.SCHEMA, Time.toLogical(Time.SCHEMA, 45_296_789), null, null, 0);

    assertEquals("45296789", KeyConverter.convert(record));
  }

  @Test
  void convertsTimestampKeyToMillisecondsSinceTheEpoch() {
    SinkRecord record =
        new SinkRecord(
            "topic",
            0,
            Timestamp.SCHEMA,
            Timestamp.toLogical(Timestamp.SCHEMA, 1_728_000_000_123L),
            null,
            null,
            0);

    assertEquals("1728000000123", KeyConverter.convert(record));
  }

  @Test
  void convertsIntegerKeyWithoutSchemaToString() {
    SinkRecord record = new SinkRecord("topic", 0, null, 1991, null, null, 0);

    assertEquals("1991", KeyConverter.convert(record));
  }
}
