package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;

/**
 * Converts records on the task thread and decides what happens to invalid messages: records whose conversion
 * throws a {@link DataException}. An invalid message is reported to the errant record reporter, then dropped or
 * failed according to {@code vespa.drop.invalid.message}.
 */
public class InvalidMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(InvalidMessageHandler.class);

    private final boolean dropInvalidMessage;
    private final ErrantRecordReporter reporter;

    /**
     * @param reporter the errant record reporter, or {@code null} when none is configured
     */
    public InvalidMessageHandler(VespaSinkConfig config, ErrantRecordReporter reporter) {
        this.dropInvalidMessage = config.dropInvalidMessage();
        this.reporter = reporter;
    }

    /**
     * Returns the converted record, or empty when the record is an invalid message that is dropped.
     *
     * @throws DataException when the record is an invalid message that is not dropped
     */
    public <T> Optional<T> convert(SinkRecord record, Function<SinkRecord, T> conversion) {
        try {
            return Optional.of(conversion.apply(record));
        } catch (DataException exception) {
            if (reporter != null) {
                reporter.report(record, exception);
            }

            // The exception message can echo record content, so only its type is logged and rethrown.
            String failure = String.format("Can't convert %s. Failure type: %s",
                    Records.describe(record), exception.getClass().getName());

            if (dropInvalidMessage) {
                log.error(failure);
                return Optional.empty();
            }

            log.trace(failure, exception);
            throw new DataException(failure);
        }
    }
}
