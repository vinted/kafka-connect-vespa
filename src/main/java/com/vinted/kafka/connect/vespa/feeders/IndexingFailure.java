package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.OperationParseException;
import ai.vespa.feed.client.ResultException;
import ai.vespa.feed.client.ResultParseException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The kind of an indexing failure, recognised from vespa-feed-client 8.781.11 exceptions, which have no status code.
 * The one place that reads the client's exception messages.
 */
public enum IndexingFailure {
    MALFORMED_DOCUMENT(VespaFeedException.Kind.MALFORMED_DOCUMENT),
    TRANSIENT(VespaFeedException.Kind.TRANSIENT),
    FATAL(VespaFeedException.Kind.FATAL);

    private static final Pattern STATUS = Pattern.compile("Status (\\d{3}) executing");
    private static final int MAX_DESCRIPTION = 300;

    private final VespaFeedException.Kind kind;

    IndexingFailure(VespaFeedException.Kind kind) {
        this.kind = kind;
    }

    /** How the feed reports this failure. */
    VespaFeedException.Kind kind() {
        return kind;
    }

    public static IndexingFailure of(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return of(failure.getCause());
        }

        if (isMalformedDocument(failure)) {
            return MALFORMED_DOCUMENT;
        }

        return failure instanceof FeedException && isTransient((FeedException) failure)
                || isUntypedHandshakeRejection(failure) ? TRANSIENT : FATAL;
    }

    // The client switches on the Content-Type of a non-OK handshake response, so a missing one throws this.
    private static boolean isUntypedHandshakeRejection(Throwable failure) {
        return failure instanceof NullPointerException
                && Arrays.stream(failure.getStackTrace()).limit(1).anyMatch(frame ->
                        frame.getClassName().equals("ai.vespa.feed.client.impl.HttpFeedClient")
                                && frame.getMethodName().equals("verifyConnection"));
    }

    private static boolean isMalformedDocument(Throwable failure) {
        String description = failure.toString().toLowerCase();

        // The response could not be parsed, so its status is unknown.
        if (description.contains("resultparseexception")) {
            return false;
        }

        Throwable rootCause = rootCause(failure);

        return description.contains("status 400")
                || description.contains("string field value contains illegal code point")
                || rootCause instanceof OperationParseException
                || rootCause instanceof JsonParseException;
    }

    private static boolean isTransient(FeedException failure) {
        // Document JSON the client could not parse; its cause is a Jackson exception, which is an IOException.
        if (failure instanceof OperationParseException) {
            return false;
        }

        // 502, 504 and 507, and responses that are not Vespa JSON, such as a load balancer's error page.
        if (failure instanceof ResultException || failure instanceof ResultParseException) {
            return true;
        }

        String message = String.valueOf(failure.getMessage());

        // The client's circuit breaker opened, or the server rejected the handshake without a status code.
        return message.contains("Operation aborted")
                || message.contains("non-OK to handshake")
                || status(failure).filter(status -> status >= 500).isPresent()
                || causes(failure.getCause()).anyMatch(IndexingFailure::isNetworkFailure);
    }

    // Mirrors the failures the feed client itself retries in HttpRequestStrategy, plus timeouts.
    private static boolean isNetworkFailure(Throwable cause) {
        return cause instanceof IOException && !(cause instanceof JsonProcessingException)
                || cause instanceof TimeoutException
                || cause instanceof IllegalStateException && "session closed".equals(cause.getMessage())
                // ai.vespa.feed.client.impl.RetryableException is package-private.
                || "RetryableException".equals(cause.getClass().getSimpleName());
    }

    /** The HTTP status the client wrote into the message of the failure or one of its causes. */
    static Optional<Integer> status(Throwable failure) {
        return causes(failure)
                .map(cause -> STATUS.matcher(String.valueOf(cause.getMessage())))
                .filter(Matcher::find)
                .map(status -> Integer.parseInt(status.group(1)))
                .findFirst();
    }

    /**
     * The type and message of the failure's root cause, without the document id the client prefixes it with and
     * truncated, to tell failures apart in logs and exception messages.
     */
    static String describe(Throwable failure) {
        Throwable rootCause = rootCause(failure);
        String message = messageWithoutDocumentId(rootCause);
        String description = rootCause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
        return description.length() <= MAX_DESCRIPTION ? description : description.substring(0, MAX_DESCRIPTION) + "...";
    }

    static Throwable rootCause(Throwable failure) {
        return causes(failure).reduce((cause, next) -> next).orElse(failure);
    }

    private static String messageWithoutDocumentId(Throwable failure) {
        String message = failure.getMessage();
        if (message == null || !(failure instanceof FeedException)) {
            return message;
        }

        String prefix = ((FeedException) failure).documentId().map(id -> "(" + id + ") ").orElse("");
        return message.startsWith(prefix) ? message.substring(prefix.length()) : message;
    }

    private static Stream<Throwable> causes(Throwable failure) {
        return Stream.iterate(failure, Objects::nonNull, Throwable::getCause);
    }
}
