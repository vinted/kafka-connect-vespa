package com.vinted.kafka.connect.vespa.feeders;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Objects;

/**
 * A record's topic, partition and offset: how the connector identifies a record. Its key and value may hold sensitive
 * data, so they never go into logs or exception messages; the errant record reporter is where the full record goes.
 */
public final class RecordCoordinates {
    private final String topic;
    private final Integer partition;
    private final long offset;

    private RecordCoordinates(String topic, Integer partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public static RecordCoordinates of(SinkRecord record) {
        return new RecordCoordinates(record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RecordCoordinates)) {
            return false;
        }

        RecordCoordinates that = (RecordCoordinates) other;
        return offset == that.offset && Objects.equals(topic, that.topic) && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset);
    }

    @Override
    public String toString() {
        return String.format("record from topic=%s partition=%s offset=%s", topic, partition, offset);
    }
}
