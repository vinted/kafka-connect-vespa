package com.vinted.kafka.connect.vespa;

import com.vinted.kafka.connect.vespa.RetryBudget.Decision;
import com.vinted.kafka.connect.vespa.RetryBudget.Outcome;
import com.vinted.kafka.connect.vespa.feeders.IndexingFailure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.TRANSIENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryBudgetTest {
    private final RetryBudget budget = new RetryBudget(2, 1000);

    @Test
    void retriesTransientFailuresWithAGrowingBackoff() {
        Decision first = budget.failed(TRANSIENT);
        Decision second = budget.failed(TRANSIENT);

        assertEquals(Outcome.RETRY, first.outcome);
        assertEquals(1, first.attempt);
        assertTrue(first.backoffMs >= 0 && first.backoffMs < 2000, "first backoff " + first.backoffMs);
        assertEquals(Outcome.RETRY, second.outcome);
        assertEquals(2, second.attempt);
        assertTrue(second.backoffMs >= 0 && second.backoffMs < 4000, "second backoff " + second.backoffMs);
    }

    @Test
    void isExhaustedAfterMaxRetries() {
        budget.failed(TRANSIENT);
        budget.failed(TRANSIENT);

        assertEquals(Outcome.EXHAUSTED, budget.failed(TRANSIENT).outcome);
    }

    @Test
    void resetsAfterASuccessfulBatch() {
        budget.failed(TRANSIENT);
        budget.failed(TRANSIENT);
        budget.succeeded();

        Decision decision = budget.failed(TRANSIENT);

        assertEquals(Outcome.RETRY, decision.outcome);
        assertEquals(1, decision.attempt);
    }

    @Test
    void resetsOnceExhausted() {
        budget.failed(TRANSIENT);
        budget.failed(TRANSIENT);
        budget.failed(TRANSIENT);

        assertEquals(1, budget.failed(TRANSIENT).attempt);
    }

    @ParameterizedTest
    @EnumSource(names = {"MALFORMED_DOCUMENT", "FATAL"})
    void neverRetriesFailuresThatAreNotTransient(IndexingFailure failure) {
        assertEquals(Outcome.NOT_TRANSIENT, budget.failed(failure).outcome);
    }

    @Test
    void neverRetriesWhenRetriesAreDisabled() {
        assertEquals(Outcome.EXHAUSTED, new RetryBudget(0, 1000).failed(TRANSIENT).outcome);
    }

    @Test
    void retriesAtOnceWithoutBackoff() {
        assertEquals(0, new RetryBudget(2, 0).failed(TRANSIENT).backoffMs);
    }
}
