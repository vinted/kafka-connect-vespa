package com.vinted.kafka.connect.vespa.feeders;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The one way a {@link VespaFeed} fails. Its message is why: each distinct reason with its kind, one example and, for
 * failed operations, how many failed for it. Its cause is the first failure.
 */
public class VespaFeedException extends ConnectException {
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
        return failure.kind + ": " + IndexingFailure.describe(failure.cause);
    }

    private static String describe(List<Failure> failures) {
        Map<String, List<Failure>> byReason = failures.stream()
                .collect(Collectors.groupingBy(VespaFeedException::reason, LinkedHashMap::new, Collectors.toList()));

        return byReason.values().stream()
                .map(same -> String.format("%s in %d %s: %s", same.get(0).kind, same.size(),
                        same.size() == 1 ? "operation" : "operations", IndexingFailure.describe(same.get(0).cause)))
                .collect(Collectors.joining("; "));
    }

    // Messages carry document ids and paths, so failures are told apart by kind, type and HTTP status only.
    private static String reason(Failure failure) {
        return failure.kind + " " + IndexingFailure.rootCause(failure.cause).getClass().getName()
                + IndexingFailure.status(failure.cause).map(status -> " " + status).orElse("");
    }
}
