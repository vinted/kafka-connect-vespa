package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.OperationParameters;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentJson;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * RAW: the value is a Vespa document JSON operation, which carries its own id, condition and create, so the key, the
 * namespace, the document type, the condition and create are not used. Tombstones carry no operation and are skipped.
 */
final class RawTranslator implements RecordTranslator<DocumentJson> {
    private static final Logger log = LoggerFactory.getLogger(RawTranslator.class);

    private final OperationParameters parameters;

    RawTranslator(VespaSinkConfig config) {
        this.parameters = RecordTranslator.commonParameters(config);
    }

    @Override
    public Optional<DocumentJson> translate(SinkRecord record) {
        if (record.value() == null) {
            log.debug("Skipping tombstone {}.", Records.describe(record));
            return Optional.empty();
        }

        String json = ValueConverter.convert(record);

        if (json.isBlank()) {
            throw new DataException("Value is empty; it must be a Vespa document operation.");
        }

        return Optional.of(new DocumentJson(record, json, parameters));
    }

    /** Sends every operation, as an update can depend on an earlier operation on the same document. */
    @Override
    public List<DocumentJson> compact(List<DocumentJson> operations) {
        return operations;
    }
}
