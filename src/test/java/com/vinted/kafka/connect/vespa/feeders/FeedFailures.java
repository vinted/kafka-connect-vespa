package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;

/** Feed failures for tests outside this package, where {@link VespaFeedException} cannot be built. */
public final class FeedFailures {
    private FeedFailures() {
    }

    public static VespaFeedException transientFailure() {
        return new VespaFeedException(VespaFeedException.Kind.TRANSIENT,
                new FeedException("Status 503 executing 'POST /document/v1/': unavailable"));
    }

    public static VespaFeedException fatalFailure() {
        return new VespaFeedException(VespaFeedException.Kind.FATAL,
                new FeedException("Status 401 executing 'POST /document/v1/': unauthorized"));
    }
}
