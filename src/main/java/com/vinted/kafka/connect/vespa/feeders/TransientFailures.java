package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.MultiFeedException;
import ai.vespa.feed.client.OperationParseException;
import ai.vespa.feed.client.ResultException;
import ai.vespa.feed.client.ResultParseException;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Recognises transient failures: Vespa or the network being unavailable, rather than anything wrong with the
 * records. Only feed client failures can be transient; anything else, such as an invalid message or a missing
 * certificate file, is not.
 */
public final class TransientFailures {
    private static final Pattern SERVER_ERROR = Pattern.compile("Status 5\\d\\d executing");

    private TransientFailures() {
    }

    public static boolean isTransient(Throwable throwable) {
        if (throwable instanceof CompletionException && throwable.getCause() != null) {
            return isTransient(throwable.getCause());
        }

        if (throwable instanceof MultiFeedException) {
            MultiFeedException failures = (MultiFeedException) throwable;
            return !failures.feedExceptions().isEmpty()
                    && failures.feedExceptions().stream().allMatch(TransientFailures::isTransient);
        }

        return throwable instanceof FeedException && isTransient((FeedException) throwable);
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
                || Stream.iterate(failure.getCause(), Objects::nonNull, Throwable::getCause)
                        .anyMatch(TransientFailures::isNetworkFailure);
    }

    /**
     * Mirrors the failures the feed client itself retries in {@code HttpRequestStrategy}, plus timeouts.
     */
    private static boolean isNetworkFailure(Throwable cause) {
        return cause instanceof IOException && !(cause instanceof JsonProcessingException)
                || cause instanceof TimeoutException
                || cause instanceof IllegalStateException && "session closed".equals(cause.getMessage())
                // ai.vespa.feed.client.impl.RetryableException is package-private.
                || "RetryableException".equals(cause.getClass().getSimpleName());
    }
}
