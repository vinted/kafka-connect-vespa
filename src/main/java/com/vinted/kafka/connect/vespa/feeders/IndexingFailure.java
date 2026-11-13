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
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** The kind of an indexing failure, recognised from vespa-feed-client 8.781.11 exceptions, which have no status code. */
public enum IndexingFailure {
    MALFORMED_DOCUMENT,
    TRANSIENT,
    FATAL;

    private static final Pattern SERVER_ERROR = Pattern.compile("Status 5\\d\\d executing");

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

        Throwable rootCause = causes(failure).reduce((cause, next) -> next).orElse(failure);

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
                || SERVER_ERROR.matcher(message).find()
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

    private static Stream<Throwable> causes(Throwable failure) {
        return Stream.iterate(failure, Objects::nonNull, Throwable::getCause);
    }
}
