package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.MultiFeedException;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VespaFeedExceptionTest {
    @Test
    void countsTheOperationsThatFailedForEachReason() {
        VespaFeedException exception = new VespaFeedException(new MultiFeedException(List.of(
                unavailable("1"),
                new FeedException(id("2"), new ConnectException("Connection refused")),
                unavailable("3"))));

        assertEquals("2 operations failed with FeedException: Status 503 executing "
                        + "'POST /document/v1/ns/dt/docid/1': unavailable; "
                        + "1 operation failed with ConnectException: Connection refused",
                exception.reasons());
    }

    @Test
    void tellsHttpStatusesApart() {
        VespaFeedException exception = new VespaFeedException(new MultiFeedException(List.of(
                unavailable("1"),
                new FeedException(id("2"), "Status 500 executing 'POST /document/v1/ns/dt/docid/2': internal"))));

        assertEquals("1 operation failed with FeedException: Status 503 executing "
                        + "'POST /document/v1/ns/dt/docid/1': unavailable; "
                        + "1 operation failed with FeedException: Status 500 executing "
                        + "'POST /document/v1/ns/dt/docid/2': internal",
                exception.reasons());
    }

    @Test
    void describesTheRootCauseOfAFailureThatIsNotABatch() {
        VespaFeedException exception = new VespaFeedException(
                new CompletionException(new FeedException(new ConnectException("Connection refused"))));

        assertEquals("ConnectException: Connection refused", exception.reasons());
    }

    @Test
    void truncatesLongMessages() {
        VespaFeedException exception = new VespaFeedException(new FeedException("x".repeat(1000)));

        assertTrue(exception.reasons().length() < 400, exception.reasons());
        assertTrue(exception.reasons().endsWith("..."), exception.reasons());
    }

    private static FeedException unavailable(String id) {
        return new FeedException(id(id), "Status 503 executing 'POST /document/v1/ns/dt/docid/" + id + "': unavailable");
    }

    private static DocumentId id(String id) {
        return DocumentId.of("ns", "dt", id);
    }
}
