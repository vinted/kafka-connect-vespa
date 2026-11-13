package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedException;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Failure;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Kind;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VespaFeedExceptionTest {
    @Test
    void countsTheOperationsThatFailedForEachReason() {
        VespaFeedException exception = new VespaFeedException(List.of(
                transientFailure(unavailable("1")),
                transientFailure(new FeedException(id("2"), new ConnectException("Connection refused"))),
                transientFailure(unavailable("3"))));

        assertEquals("transient failure in 2 operations: FeedException: Status 503 executing "
                        + "'POST /document/v1/ns/dt/docid/1': unavailable; "
                        + "transient failure in 1 operation: ConnectException: Connection refused",
                exception.getMessage());
    }

    @Test
    void tellsHttpStatusesApart() {
        VespaFeedException exception = new VespaFeedException(List.of(
                transientFailure(unavailable("1")),
                transientFailure(new FeedException(id("2"), "Status 500 executing 'POST /document/v1/ns/dt/docid/2': internal"))));

        assertEquals("transient failure in 1 operation: FeedException: Status 503 executing "
                        + "'POST /document/v1/ns/dt/docid/1': unavailable; "
                        + "transient failure in 1 operation: FeedException: Status 500 executing "
                        + "'POST /document/v1/ns/dt/docid/2': internal",
                exception.getMessage());
    }

    @Test
    void tellsKindsApart() {
        FeedException rejected = new FeedException(id("1"), "Result{type=conditionNotMet}");
        FeedException badRequest = new FeedException(id("2"), "Status 400 executing 'POST /document/v1/ns/dt/docid/2': bad");

        VespaFeedException exception = new VespaFeedException(List.of(
                new Failure(Kind.REJECTED_RESULT, rejected),
                new Failure(Kind.MALFORMED_DOCUMENT, badRequest)));

        assertEquals("rejected result in 1 operation: FeedException: Result{type=conditionNotMet}; "
                        + "malformed document in 1 operation: FeedException: Status 400 executing "
                        + "'POST /document/v1/ns/dt/docid/2': bad",
                exception.getMessage());
        assertSame(rejected, exception.getCause());
    }

    @Test
    void isTransientOnlyWhenEveryFailureIs() {
        assertTrue(new VespaFeedException(List.of(transientFailure(unavailable("1")), transientFailure(unavailable("2")))).isTransient());
        assertFalse(new VespaFeedException(List.of(
                transientFailure(unavailable("1")),
                new Failure(Kind.MALFORMED_DOCUMENT, new FeedException(id("2"), "Status 400 executing")))).isTransient());
    }

    @Test
    void describesTheRootCauseOfAFailureBeforeSending() {
        VespaFeedException exception = new VespaFeedException(Kind.TRANSIENT,
                new CompletionException(new FeedException(new ConnectException("Connection refused"))));

        assertEquals("transient failure: ConnectException: Connection refused", exception.getMessage());
        assertTrue(exception.isTransient());
    }

    @Test
    void truncatesLongMessages() {
        VespaFeedException exception = new VespaFeedException(Kind.FATAL, new FeedException("x".repeat(1000)));

        assertTrue(exception.getMessage().length() < 400, exception.getMessage());
        assertTrue(exception.getMessage().endsWith("..."), exception.getMessage());
    }

    private static Failure transientFailure(Throwable cause) {
        return new Failure(Kind.TRANSIENT, cause);
    }

    private static FeedException unavailable(String id) {
        return new FeedException(id(id), "Status 503 executing 'POST /document/v1/ns/dt/docid/" + id + "': unavailable");
    }

    private static DocumentId id(String id) {
        return DocumentId.of("ns", "dt", id);
    }
}
