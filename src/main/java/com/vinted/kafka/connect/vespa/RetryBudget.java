package com.vinted.kafka.connect.vespa;

import com.vinted.kafka.connect.vespa.feeders.IndexingFailure;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/** Redeliveries of a batch that failed transiently, their backoff, and the records already reported meanwhile. */
final class RetryBudget {
    enum Outcome {
        RETRY,
        EXHAUSTED,
        NOT_TRANSIENT
    }

    /** What to do with a failed batch; the attempt and backoff only mean something for a retry. */
    static final class Decision {
        final Outcome outcome;
        final int attempt;
        final long backoffMs;

        private Decision(Outcome outcome, int attempt, long backoffMs) {
            this.outcome = outcome;
            this.attempt = attempt;
            this.backoffMs = backoffMs;
        }
    }

    private final int maxRetries;
    private final long initialBackoffMs;
    // Written from feed client callback threads as well as the task thread.
    private final Set<List<Object>> reported = ConcurrentHashMap.newKeySet();
    private int attempt;

    RetryBudget(int maxRetries, long initialBackoffMs) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
    }

    int maxRetries() {
        return maxRetries;
    }

    /** Reports each record once across the redeliveries of its batch; {@code null} when no reporter is configured. */
    ErrantRecordReporter reportingOnce(ErrantRecordReporter reporter) {
        if (reporter == null) {
            return null;
        }

        return (record, error) -> reported.add(coordinates(record))
                ? reporter.report(record, error)
                : CompletableFuture.completedFuture(null);
    }

    void succeeded() {
        reset();
    }

    /** Spends one retry when the batch failed only transiently and the budget lasts; otherwise resets the budget. */
    Decision failed(IndexingFailure failure) {
        if (failure != IndexingFailure.TRANSIENT) {
            return giveUp(Outcome.NOT_TRANSIENT);
        }

        if (attempt >= maxRetries) {
            return giveUp(Outcome.EXHAUSTED);
        }

        attempt++;
        return new Decision(Outcome.RETRY, attempt, Backoff.randomBackoffMs(attempt, initialBackoffMs));
    }

    private Decision giveUp(Outcome outcome) {
        reset();
        return new Decision(outcome, 0, 0);
    }

    private void reset() {
        attempt = 0;
        reported.clear();
    }

    private static List<Object> coordinates(SinkRecord record) {
        return Arrays.asList(record.topic(), record.kafkaPartition(), record.kafkaOffset());
    }
}
