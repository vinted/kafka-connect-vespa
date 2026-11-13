package com.vinted.kafka.connect.vespa;

import com.vinted.kafka.connect.vespa.RetryBudget.Decision;
import com.vinted.kafka.connect.vespa.RetryBudget.Outcome;
import com.vinted.kafka.connect.vespa.feeders.IndexingFailure;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.TRANSIENT;
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

    @Test
    void reportsARecordOnceWhileItsBatchIsRetried() {
        ErrantRecordReporter reportingOnce = budget.reportingOnce(reporter);

        reportingOnce.report(record("topic", 0, 7), ERROR);
        budget.failed(TRANSIENT);
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
    @EnumSource(IndexingFailure.class)
    void reportsARecordAgainOnceTheBudgetGivesUp(IndexingFailure failure) {
        RetryBudget noRetries = new RetryBudget(0, 1000);
        ErrantRecordReporter reportingOnce = noRetries.reportingOnce(reporter);

        reportingOnce.report(record("topic", 0, 7), ERROR);
        noRetries.failed(failure);
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
