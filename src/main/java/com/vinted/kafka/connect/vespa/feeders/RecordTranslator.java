package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Optional;

/** How one operational mode translates records into Vespa operations. */
interface RecordTranslator<T extends VespaOperation> {
    static RecordTranslator<?> of(VespaSinkConfig config) {
        switch (config.operationalMode()) {
            case UPSERT:
                return new UpsertTranslator(config);
            case RAW:
                return new RawTranslator();
            default:
                throw new ConnectException(String.format("Unknown operational mode: %s", config.operationalMode()));
        }
    }

    /**
     * Returns the record's operation, or empty when the mode skips the record.
     *
     * @throws DataException when the record is an invalid message
     */
    Optional<T> translate(SinkRecord record);

    /** Returns which operations of a batch to send, in order. */
    List<T> compact(List<T> operations);
}
