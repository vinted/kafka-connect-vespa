package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The one way a {@link VespaFeed} fails. Its message is why: each distinct reason with its kind, one example and, for
 * failed operations, how many failed for it. Its cause is the first failure.
 */
public class VespaFeedException extends ConnectException {
    private static final Pattern STATUS = Pattern.compile("Status (\\d{3}) executing");
    private static final int MAX_DESCRIPTION = 300;

    /** What made the feed fail. */
    public enum Kind {
        INVALID_MESSAGE("invalid message"),
        MALFORMED_DOCUMENT("malformed document"),
        REJECTED_RESULT("rejected result"),
        TRANSIENT("transient failure"),
        FATAL("fatal failure");

        private final String description;

        Kind(String description) {
            this.description = description;
        }

        static Kind of(IndexingFailure failure) {
            switch (failure) {
                case MALFORMED_DOCUMENT:
                    return MALFORMED_DOCUMENT;
                case TRANSIENT:
                    return TRANSIENT;
                case FATAL:
                default:
                    return FATAL;
            }
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /** One failure of a feed: a failed operation, or the feed itself. */
    static final class Failure {
        final Kind kind;
        final Throwable cause;

        Failure(Kind kind, Throwable cause) {
            this.kind = kind;
            this.cause = cause;
        }
    }

    private final boolean isTransient;

    /** The feed failed before sending its operations. */
    VespaFeedException(Kind kind, Throwable cause) {
        super(describe(new Failure(kind, cause)), cause);
        this.isTransient = kind == Kind.TRANSIENT;
    }

    /** Operations of the feed failed. */
    VespaFeedException(List<Failure> failures) {
        super(describe(failures), failures.get(0).cause);
        this.isTransient = failures.stream().allMatch(failure -> failure.kind == Kind.TRANSIENT);
    }

    /** True when every failure was transient, so feeding the same records again may succeed. */
    public boolean isTransient() {
        return isTransient;
    }

    private static String describe(Failure failure) {
        return failure.kind + ": " + summary(rootCause(failure.cause));
    }

    private static String describe(List<Failure> failures) {
        Map<String, List<Failure>> byReason = failures.stream()
                .collect(Collectors.groupingBy(VespaFeedException::reason, LinkedHashMap::new, Collectors.toList()));

        return byReason.values().stream()
                .map(same -> String.format("%s in %d %s: %s", same.get(0).kind, same.size(),
                        same.size() == 1 ? "operation" : "operations", summary(rootCause(same.get(0).cause))))
                .collect(Collectors.joining("; "));
    }

    // Messages carry document ids and paths, so failures are told apart by kind, type and HTTP status only.
    private static String reason(Failure failure) {
        Throwable rootCause = rootCause(failure.cause);
        Matcher status = STATUS.matcher(String.valueOf(rootCause.getMessage()));
        return failure.kind + " " + rootCause.getClass().getName() + (status.find() ? " " + status.group(1) : "");
    }

    private static String summary(Throwable failure) {
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
