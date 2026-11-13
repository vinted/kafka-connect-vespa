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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.FATAL;
import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.MALFORMED_DOCUMENT;
import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.TRANSIENT;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexingFailureTest {
    private static final DocumentId ID = DocumentId.of("id:ns:dt::1");
    private static final FeedException BAD_REQUEST = new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request");
    private static final FeedException UNAVAILABLE = new FeedException(ID, "Status 503 executing 'POST /document/v1/': unavailable");
    private static final FeedException UNAUTHORIZED = new FeedException(ID, "Status 401 executing 'POST /document/v1/': unauthorized");

    static Stream<Arguments> failures() {
        return Stream.of(
                // Malformed documents
                Arguments.of("400", BAD_REQUEST, MALFORMED_DOCUMENT),
                Arguments.of("illegal code point", new FeedException(ID, "String field value contains illegal code point 0x1"), MALFORMED_DOCUMENT),
                Arguments.of("unparseable operation", new OperationParseException("No document id for document at offset 0"), MALFORMED_DOCUMENT),
                Arguments.of("unparseable document JSON", new OperationParseException("Failed to parse document JSON", new JsonParseException(null, "Unexpected character")), MALFORMED_DOCUMENT),
                Arguments.of("400 despite a network cause", new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request", new IOException("reset")), MALFORMED_DOCUMENT),
                Arguments.of("completion wrapper of 400", new CompletionException(BAD_REQUEST), MALFORMED_DOCUMENT),
                Arguments.of("all malformed in batch", new MultiFeedException(List.of(BAD_REQUEST, BAD_REQUEST)), MALFORMED_DOCUMENT),
                // Network and unreachable host
                Arguments.of("connection refused", new FeedException(ID, new java.net.ConnectException("Connection refused")), TRANSIENT),
                Arguments.of("unknown host", new FeedException(ID, new UnknownHostException("vespa")), TRANSIENT),
                Arguments.of("timeout", new FeedException(ID, new TimeoutException("operation timed out after 'PT1M'")), TRANSIENT),
                Arguments.of("HTTP/2 session closed", new FeedException(ID, new IllegalStateException("session closed")), TRANSIENT),
                Arguments.of("handshake failed", new FeedException("failed handshake with server after PT30S", new java.net.ConnectException("Connection refused")), TRANSIENT),
                Arguments.of("handshake timed out", new FeedException("timed out during handshake with server", new TimeoutException()), TRANSIENT),
                // Server errors
                Arguments.of("503", UNAVAILABLE, TRANSIENT),
                Arguments.of("500", new FeedException(ID, "Status 500 executing 'POST /document/v1/': internal error"), TRANSIENT),
                Arguments.of("502, 504, 507", new ResultException(ID, "insufficient storage", null), TRANSIENT),
                Arguments.of("load balancer error page", new ResultParseException(ID, "Expected 'START_OBJECT', but found 'null' in: <html>502 Bad Gateway</html>"), TRANSIENT),
                Arguments.of("unparseable result, even with status 400", new ResultParseException(ID, "Status 400: failed parsing response"), TRANSIENT),
                Arguments.of("handshake rejected", new FeedException("server responded non-OK to handshake: Bad Gateway"), TRANSIENT),
                Arguments.of("handshake rejected without a content type", nullPointerAt("ai.vespa.feed.client.impl.HttpFeedClient", "verifyConnection"), TRANSIENT),
                Arguments.of("circuit breaker open", new FeedException("Operation aborted"), TRANSIENT),
                Arguments.of("all transient in batch", new MultiFeedException(List.of(UNAVAILABLE, UNAVAILABLE)), TRANSIENT),
                Arguments.of("completion wrapper", new CompletionException(UNAVAILABLE), TRANSIENT),
                // Anything else
                Arguments.of("401", UNAUTHORIZED, FATAL),
                Arguments.of("404", new FeedException(ID, "Status 404 executing 'POST /document/v1/': not found"), FATAL),
                Arguments.of("condition not met", new FeedException(ID, "Result{type=conditionNotMet}"), FATAL),
                Arguments.of("unparseable operation with a network cause", new OperationParseException("Failed to read document", new IOException("reset")), FATAL),
                Arguments.of("malformed document id in RAW mode", new FeedException(new IllegalArgumentException("Document ID must be on the form 'id:...'")), FATAL),
                Arguments.of("reporter tolerance exceeded", new FeedException("Failed to handle the indexing result", new ConnectException("Tolerance exceeded in error handler")), FATAL),
                Arguments.of("mixed kinds in batch", new MultiFeedException(List.of(UNAVAILABLE, BAD_REQUEST)), FATAL),
                Arguments.of("any fatal in batch", new MultiFeedException(List.of(UNAVAILABLE, UNAUTHORIZED)), FATAL),
                Arguments.of("empty batch failure", new MultiFeedException(List.of()), FATAL),
                Arguments.of("invalid message", new DataException("Key is used as document id and can not be null."), FATAL),
                Arguments.of("null pointer outside the handshake", nullPointerAt("ai.vespa.feed.client.impl.HttpFeedClient", "send"), FATAL),
                Arguments.of("missing certificate file", new UncheckedIOException(new NoSuchFileException("client.pem")), FATAL)
        );
    }

    @ParameterizedTest(name = "{0} -> {2}")
    @MethodSource("failures")
    void classifiesFailures(String name, Throwable failure, IndexingFailure expected) {
        assertEquals(expected, IndexingFailure.of(failure));
    }

    private static NullPointerException nullPointerAt(String className, String methodName) {
        NullPointerException failure = new NullPointerException();
        failure.setStackTrace(new StackTraceElement[]{new StackTraceElement(className, methodName, null, -1)});
        return failure;
    }
}
