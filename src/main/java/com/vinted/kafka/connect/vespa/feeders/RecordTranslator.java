package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.OperationParameters;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Optional;

/** How one operational mode translates records into Vespa operations. */
interface RecordTranslator<T extends VespaOperation> {
    static RecordTranslator<?> of(VespaSinkConfig config) {
        switch (config.operationalMode) {
            case UPSERT:
                return new UpsertTranslator(config);
            case RAW:
                return new RawTranslator(config);
            default:
                throw new ConnectException(String.format("Unknown operational mode: %s", config.operationalMode));
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

    /** The operation parameters every mode honours: timeout, route and trace level. */
    static OperationParameters commonParameters(VespaSinkConfig config) {
        OperationParameters parameters = OperationParameters.empty();

        if (config.timeout.toMillis() > 0) {
            parameters = parameters.timeout(config.timeout);
        }

        if (!Strings.isNullOrEmpty(config.route)) {
            parameters = parameters.route(config.route);
        }

        if (config.tracelevel > 0) {
            parameters = parameters.tracelevel(config.tracelevel);
        }

        return parameters;
    }
}
