package com.vinted.kafka.connect.vespa.feeders;

import org.apache.kafka.connect.sink.SinkRecord;

final class Records {
    private Records() {
    }

    /**
     * Identifies a record by its coordinates only. Its key and value may hold sensitive data, so they never go into
     * logs or exception messages; the errant record reporter is where the full record goes.
     */
    static String describe(SinkRecord record) {
        return String.format("record from topic=%s partition=%s offset=%s",
                record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }
}
