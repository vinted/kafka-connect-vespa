package com.vinted.kafka.connect.vespa.feeders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class RecordCoordinatesTest {
  @Test
  void identifiesRecordByTopicPartitionAndOffsetOnly() {
    RecordCoordinates coordinates = RecordCoordinates.of(record("topic", 0, 7, "key", "{}"));

    assertEquals(
        coordinates, RecordCoordinates.of(record("topic", 0, 7, "other key", "{\"other\":1}")));
    assertEquals(
        coordinates.hashCode(),
        RecordCoordinates.of(record("topic", 0, 7, "other key", "{}")).hashCode());
    assertNotEquals(coordinates, RecordCoordinates.of(record("other", 0, 7, "key", "{}")));
    assertNotEquals(coordinates, RecordCoordinates.of(record("topic", 1, 7, "key", "{}")));
    assertNotEquals(coordinates, RecordCoordinates.of(record("topic", 0, 8, "key", "{}")));
    assertEquals(
        4,
        Set.of(
                coordinates,
                RecordCoordinates.of(record("other", 0, 7, "key", "{}")),
                RecordCoordinates.of(record("topic", 1, 7, "key", "{}")),
                RecordCoordinates.of(record("topic", 0, 8, "key", "{}")))
            .size());
  }

  @Test
  void describesTheRecordWithoutItsKeyOrValue() {
    String description =
        RecordCoordinates.of(record("topic", 2, 9, "secret-key", "{\"secret\":1}")).toString();

    assertEquals("record from topic=topic partition=2 offset=9", description);
  }

  private static SinkRecord record(
      String topic, int partition, long offset, String key, String value) {
    return new SinkRecord(
        topic, partition, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, value, offset);
  }
}
