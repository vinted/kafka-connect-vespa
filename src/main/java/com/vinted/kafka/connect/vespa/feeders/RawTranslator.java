package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentJson;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * RAW: the value is a Vespa document JSON operation, which carries its own id, condition and create, so the key, the
 * namespace, the document type, the condition and create are not used. Tombstones carry no operation and are skipped.
 */
final class RawTranslator extends RecordTranslator<DocumentJson> {
    private static final Logger log = LoggerFactory.getLogger(RawTranslator.class);

    RawTranslator(boolean dropInvalidMessage, ErrantRecordReporter reporter) {
        super(dropInvalidMessage, reporter);
    }

    @Override
    Optional<DocumentJson> translate(SinkRecord record) {
        if (record.value() == null) {
            log.debug("Skipping tombstone {}.", RecordCoordinates.of(record));
            return Optional.empty();
        }

        String json = ValueConverter.convert(record);

        if (json.isBlank()) {
            throw new DataException("Value is empty; it must be a Vespa document operation.");
        }

        return Optional.of(new DocumentJson(record, json));
    }

    /** Sends every operation, as an update can depend on an earlier operation on the same document. */
    @Override
    List<DocumentJson> compact(List<DocumentJson> operations) {
        return operations;
    }
}
