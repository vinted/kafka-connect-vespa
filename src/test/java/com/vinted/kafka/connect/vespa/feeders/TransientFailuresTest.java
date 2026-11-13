package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.MultiFeedException;
import ai.vespa.feed.client.OperationParseException;
import ai.vespa.feed.client.ResultException;
import ai.vespa.feed.client.ResultParseException;
import com.fasterxml.jackson.core.JsonParseException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransientFailuresTest {
    private static final DocumentId ID = DocumentId.of("id:ns:dt::1");
    private static final FeedException UNAVAILABLE = new FeedException(ID, "Status 503 executing 'POST /document/v1/': unavailable");
    private static final FeedException UNAUTHORIZED = new FeedException(ID, "Status 401 executing 'POST /document/v1/': unauthorized");

    static Stream<Arguments> failures() {
        return Stream.of(
                // Network and unreachable host
                Arguments.of("connection refused", new FeedException(ID, new java.net.ConnectException("Connection refused")), true),
                Arguments.of("unknown host", new FeedException(ID, new UnknownHostException("vespa")), true),
                Arguments.of("timeout", new FeedException(ID, new TimeoutException("operation timed out after 'PT1M'")), true),
                Arguments.of("HTTP/2 session closed", new FeedException(ID, new IllegalStateException("session closed")), true),
                Arguments.of("handshake failed", new FeedException("failed handshake with server after PT30S", new java.net.ConnectException("Connection refused")), true),
                Arguments.of("handshake timed out", new FeedException("timed out during handshake with server", new TimeoutException()), true),
                // Server errors
                Arguments.of("503", UNAVAILABLE, true),
                Arguments.of("500", new FeedException(ID, "Status 500 executing 'POST /document/v1/': internal error"), true),
                Arguments.of("502, 504, 507", new ResultException(ID, "insufficient storage", null), true),
                Arguments.of("load balancer error page", new ResultParseException(ID, "Expected 'START_OBJECT', but found 'null' in: <html>502 Bad Gateway</html>"), true),
                Arguments.of("handshake rejected", new FeedException("server responded non-OK to handshake: Bad Gateway"), true),
                Arguments.of("circuit breaker open", new FeedException("Operation aborted"), true),
                Arguments.of("all transient", new MultiFeedException(List.of(UNAVAILABLE, UNAVAILABLE)), true),
                Arguments.of("completion wrapper", new CompletionException(UNAVAILABLE), true),
                // Client errors and bad records
                Arguments.of("400", new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request"), false),
                Arguments.of("401", UNAUTHORIZED, false),
                Arguments.of("404", new FeedException(ID, "Status 404 executing 'POST /document/v1/': not found"), false),
                Arguments.of("condition not met", new FeedException(ID, "Result{type=conditionNotMet}"), false),
                Arguments.of("unparseable document", new OperationParseException("Failed to parse document JSON", new JsonParseException(null, "Unexpected character")), false),
                Arguments.of("malformed document id", new FeedException(new IllegalArgumentException("Document ID must be on the form 'id:...'")), false),
                Arguments.of("reporter tolerance exceeded", new FeedException("Failed to handle the indexing result", new ConnectException("Tolerance exceeded in error handler")), false),
                Arguments.of("any non-transient in batch", new MultiFeedException(List.of(UNAVAILABLE, UNAUTHORIZED)), false),
                Arguments.of("empty batch failure", new MultiFeedException(List.of()), false),
                Arguments.of("invalid message", new DataException("Key is used as document id and can not be null."), false),
                Arguments.of("missing certificate file", new UncheckedIOException(new NoSuchFileException("client.pem")), false)
        );
    }

    @ParameterizedTest(name = "{0} -> transient: {2}")
    @MethodSource("failures")
    void classifiesFailures(String name, Throwable failure, boolean expected) {
        assertEquals(expected, TransientFailures.isTransient(failure));
    }
}
