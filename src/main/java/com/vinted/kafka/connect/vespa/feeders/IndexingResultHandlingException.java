package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;
import org.apache.kafka.connect.sink.SinkRecord;

/** A failure to handle an indexing result, such as the errant record reporter throwing; always a fatal failure. */
final class IndexingResultHandlingException extends FeedException {
    IndexingResultHandlingException(SinkRecord record, Throwable cause) {
        super("Failed to handle the indexing result of " + Records.describe(record), cause);
    }
}
