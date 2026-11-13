package com.vinted.kafka.connect.vespa;

import com.vinted.kafka.connect.vespa.feeders.IndexingFailure;

/**
 * How many times in a row a batch that failed only with transient failures is redelivered, and how long to wait
 * before each redelivery. Reset after every successful batch.
 */
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
    private int attempt;

    RetryBudget(int maxRetries, long initialBackoffMs) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
    }

    void succeeded() {
        attempt = 0;
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
        attempt = 0;
        return new Decision(outcome, 0, 0);
    }
}
