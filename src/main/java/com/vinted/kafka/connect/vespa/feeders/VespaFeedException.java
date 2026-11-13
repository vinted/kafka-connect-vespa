package com.vinted.kafka.connect.vespa.feeders;

import org.apache.kafka.connect.errors.ConnectException;

/** The one way a {@link VespaFeed} fails; the cause is the original failure. */
public class VespaFeedException extends ConnectException {
    private final IndexingFailure failure;

    VespaFeedException(Throwable cause) {
        super(cause);
        this.failure = IndexingFailure.of(cause);
    }

    public IndexingFailure failure() {
        return failure;
    }
}
