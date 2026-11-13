package com.vinted.kafka.connect.vespa;

import com.vinted.kafka.connect.vespa.RetryBudget.Decision;
import com.vinted.kafka.connect.vespa.RetryBudget.Outcome;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RetryBudgetTest {
    private static final DataException ERROR = new DataException("invalid");

    private final RetryBudget budget = new RetryBudget(2, 1000);
    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    @Test
    void retriesTransientFailuresWithAGrowingBackoff() {
        Decision first = budget.failed(true);
        Decision second = budget.failed(true);

        assertEquals(Outcome.RETRY, first.outcome);
        assertEquals(1, first.attempt);
        assertTrue(first.backoffMs >= 0 && first.backoffMs < 2000, "first backoff " + first.backoffMs);
        assertEquals(Outcome.RETRY, second.outcome);
        assertEquals(2, second.attempt);
        assertTrue(second.backoffMs >= 0 && second.backoffMs < 4000, "second backoff " + second.backoffMs);
    }

    @Test
    void isExhaustedAfterMaxRetries() {
        budget.failed(true);
        budget.failed(true);

        assertEquals(Outcome.EXHAUSTED, budget.failed(true).outcome);
    }

    @Test
    void resetsAfterASuccessfulBatch() {
        budget.failed(true);
        budget.failed(true);
        budget.succeeded();

        Decision decision = budget.failed(true);

        assertEquals(Outcome.RETRY, decision.outcome);
        assertEquals(1, decision.attempt);
    }

    @Test
    void resetsOnceExhausted() {
        budget.failed(true);
        budget.failed(true);
        budget.failed(true);

        assertEquals(1, budget.failed(true).attempt);
    }

    @Test
    void neverRetriesFailuresThatAreNotTransient() {
        assertEquals(Outcome.NOT_TRANSIENT, budget.failed(false).outcome);
    }

    @Test
    void neverRetriesWhenRetriesAreDisabled() {
        assertEquals(Outcome.EXHAUSTED, new RetryBudget(0, 1000).failed(true).outcome);
    }

    @Test
    void retriesAtOnceWithoutBackoff() {
        assertEquals(0, new RetryBudget(2, 0).failed(true).backoffMs);
    }

    @Test
    void reportsARecordOnceWhileItsBatchIsRetried() {
        ErrantRecordReporter reportingOnce = budget.reportingOnce(reporter);

        reportingOnce.report(record("topic", 0, 7), ERROR);
        budget.failed(true);
        reportingOnce.report(record("topic", 0, 7), ERROR);

        verify(reporter, times(1)).report(any(), any());
    }

    @Test
    void reportsARecordAgainOnceItsBatchSucceeded() {
        ErrantRecordReporter reportingOnce = budget.reportingOnce(reporter);

        reportingOnce.report(record("topic", 0, 7), ERROR);
        budget.succeeded();
        reportingOnce.report(record("topic", 0, 7), ERROR);

        verify(reporter, times(2)).report(any(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void reportsARecordAgainOnceTheBudgetGivesUp(boolean isTransient) {
        RetryBudget noRetries = new RetryBudget(0, 1000);
        ErrantRecordReporter reportingOnce = noRetries.reportingOnce(reporter);

        reportingOnce.report(record("topic", 0, 7), ERROR);
        noRetries.failed(isTransient);
        reportingOnce.report(record("topic", 0, 7), ERROR);

        verify(reporter, times(2)).report(any(), any());
    }

    @Test
    void tellsRecordsApartByTopicPartitionAndOffset() {
        ErrantRecordReporter reportingOnce = budget.reportingOnce(reporter);

        reportingOnce.report(record("topic", 0, 7), ERROR);
        reportingOnce.report(record("other", 0, 7), ERROR);
        reportingOnce.report(record("topic", 1, 7), ERROR);
        reportingOnce.report(record("topic", 0, 8), ERROR);

        verify(reporter, times(4)).report(any(), any());
    }

    @Test
    void reportsARecordOnceAcrossThreads() throws InterruptedException {
        ErrantRecordReporter reportingOnce = budget.reportingOnce(reporter);
        ExecutorService threads = Executors.newFixedThreadPool(8);

        for (int i = 0; i < 64; i++) {
            threads.execute(() -> reportingOnce.report(record("topic", 0, 7), ERROR));
        }
        threads.shutdown();

        assertTrue(threads.awaitTermination(10, TimeUnit.SECONDS));
        verify(reporter, times(1)).report(any(), any());
    }

    @Test
    void hasNoReporterWhenNoneIsConfigured() {
        assertNull(budget.reportingOnce(null));
    }

    private static SinkRecord record(String topic, int partition, long offset) {
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "{}", offset);
    }
}
