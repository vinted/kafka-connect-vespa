package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Kind;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An operational mode: which Vespa operations a batch of records becomes. Invalid messages, records whose translation
 * throws a {@link DataException}, are reported to the errant record reporter, then dropped or failed according to
 * {@code vespa.drop.invalid.message}.
 */
abstract class RecordTranslator<T extends VespaOperation> {
    private static final Logger log = LoggerFactory.getLogger(RecordTranslator.class);

    private final boolean dropInvalidMessage;
    private final ErrantRecordReporter reporter;

    RecordTranslator(boolean dropInvalidMessage, ErrantRecordReporter reporter) {
        this.dropInvalidMessage = dropInvalidMessage;
        this.reporter = reporter;
    }

    static RecordTranslator<?> of(VespaSinkConfig config, ErrantRecordReporter reporter) {
        switch (config.operationalMode()) {
            case UPSERT:
                return new UpsertTranslator(config, reporter);
            case RAW:
                return new RawTranslator(config.dropInvalidMessage(), reporter);
            default:
                throw new ConnectException(String.format("Unknown operational mode: %s", config.operationalMode()));
        }
    }

    /**
     * Returns the operations to send for the records, in order.
     *
     * @throws VespaFeedException of an invalid message, when a record is an invalid message that is not dropped
     */
    final List<T> operations(Collection<SinkRecord> records) {
        List<T> operations = records.stream()
                .flatMap(record -> operation(record).stream())
                .collect(Collectors.toList());

        return compact(operations);
    }

    /**
     * Returns the record's operation, or empty when the mode skips the record.
     *
     * @throws DataException when the record is an invalid message
     */
    abstract Optional<T> translate(SinkRecord record);

    /** Returns which operations of a batch to send, in order. */
    abstract List<T> compact(List<T> operations);

    private Optional<T> operation(SinkRecord record) {
        try {
            return translate(record);
        } catch (DataException exception) {
            return invalidMessage(record, exception);
        }
    }

    private Optional<T> invalidMessage(SinkRecord record, DataException exception) {
        reporter.report(record, exception);

        // The exception message can echo record content, so only its type is logged and rethrown.
        String failure = String.format("Can't convert %s. Failure type: %s",
                RecordCoordinates.of(record), exception.getClass().getName());

        if (dropInvalidMessage) {
            log.error(failure);
            return Optional.empty();
        }

        log.trace(failure, exception);
        throw new VespaFeedException(Kind.INVALID_MESSAGE, new DataException(failure));
    }
}
