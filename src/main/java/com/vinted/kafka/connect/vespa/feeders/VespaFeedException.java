package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.MultiFeedException;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** The one way a {@link VespaFeed} fails; the cause is the original failure. */
public class VespaFeedException extends ConnectException {
    private static final Pattern STATUS = Pattern.compile("Status (\\d{3}) executing");
    private static final int MAX_DESCRIPTION = 300;

    private final IndexingFailure failure;

    VespaFeedException(Throwable cause) {
        super(cause);
        this.failure = IndexingFailure.of(cause);
    }

    public IndexingFailure failure() {
        return failure;
    }

    /** Why the feed failed: each distinct reason, with one example and how many operations failed for it. */
    public String reasons() {
        if (!(getCause() instanceof MultiFeedException)) {
            return describe(rootCause(getCause()));
        }

        Map<String, List<Throwable>> byReason = ((MultiFeedException) getCause()).feedExceptions().stream()
                .map(VespaFeedException::rootCause)
                .collect(Collectors.groupingBy(VespaFeedException::reason, LinkedHashMap::new, Collectors.toList()));

        return byReason.values().stream()
                .map(failures -> String.format("%d %s failed with %s",
                        failures.size(), failures.size() == 1 ? "operation" : "operations", describe(failures.get(0))))
                .collect(Collectors.joining("; "));
    }

    // Messages carry document ids and paths, so failures are told apart by type and HTTP status only.
    private static String reason(Throwable failure) {
        Matcher status = STATUS.matcher(String.valueOf(failure.getMessage()));
        return failure.getClass().getName() + (status.find() ? " " + status.group(1) : "");
    }

    private static String describe(Throwable failure) {
        String message = messageWithoutDocumentId(failure);
        String description = failure.getClass().getSimpleName() + (message == null ? "" : ": " + message);
        return description.length() <= MAX_DESCRIPTION ? description : description.substring(0, MAX_DESCRIPTION) + "...";
    }

    private static String messageWithoutDocumentId(Throwable failure) {
        String message = failure.getMessage();
        if (message == null || !(failure instanceof FeedException)) {
            return message;
        }

        String prefix = ((FeedException) failure).documentId().map(id -> "(" + id + ") ").orElse("");
        return message.startsWith(prefix) ? message.substring(prefix.length()) : message;
    }

    private static Throwable rootCause(Throwable failure) {
        return Stream.iterate(failure, Objects::nonNull, Throwable::getCause).reduce((cause, next) -> next).orElse(failure);
    }
}
