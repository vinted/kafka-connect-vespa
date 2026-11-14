package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RetryBudgetTest {
    private static final DataException ERROR = new DataException("invalid");
    private static final SinkRecord RECORD = record("topic", 0, 7);
    private static final List<SinkRecord> BATCH = List.of(RECORD);

    private final List<Long> backoffs = new ArrayList<>();
    private final RetryBudget budget = new RetryBudget(2, 1000, backoffs::add);
    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);

    @Test
    void feedsTheRecords() {
        List<Collection<SinkRecord>> fed = new ArrayList<>();

        budget.deliver(BATCH, fed::add);

        assertEquals(List.of(BATCH), fed);
        assertEquals(List.of(), backoffs);
    }

    @Test
    void backsOffBeforeEachRetryOfATransientFailureWithAGrowingBackoff() {
        assertRetries(() -> budget.deliver(BATCH, failing(transientFailure())));
        assertRetries(() -> budget.deliver(BATCH, failing(transientFailure())));

        assertEquals(2, backoffs.size());
        assertTrue(backoffs.get(0) >= 0 && backoffs.get(0) < 2000, "first backoff " + backoffs.get(0));
        assertTrue(backoffs.get(1) >= 0 && backoffs.get(1) < 4000, "second backoff " + backoffs.get(1));
    }

    @Test
    void failsWithTheFeedFailureOnceExhausted() {
        VespaFeedException failure = transientFailure();
        budget.deliver(BATCH, records -> { });
        assertRetries(() -> budget.deliver(BATCH, failing(failure)));
        assertRetries(() -> budget.deliver(BATCH, failing(failure)));

        assertSame(failure, assertThrows(VespaFeedException.class, () -> budget.deliver(BATCH, failing(failure))));
        assertEquals(2, backoffs.size());
    }

    @Test
    void startsAgainAfterASuccessfulBatch() {
        assertRetries(() -> budget.deliver(BATCH, failing(transientFailure())));
        assertRetries(() -> budget.deliver(BATCH, failing(transientFailure())));
        budget.deliver(BATCH, records -> { });

        assertRetries(() -> budget.deliver(BATCH, failing(transientFailure())));
    }

    @Test
    void startsAgainOnceExhausted() {
        assertRetries(() -> budget.deliver(BATCH, failing(transientFailure())));
        assertRetries(() -> budget.deliver(BATCH, failing(transientFailure())));
        assertThrows(VespaFeedException.class, () -> budget.deliver(BATCH, failing(transientFailure())));

        assertRetries(() -> budget.deliver(BATCH, failing(transientFailure())));
    }

    @Test
    void neverRetriesFailuresThatAreNotTransient() {
        VespaFeedException failure = fatalFailure();

        assertSame(failure, assertThrows(VespaFeedException.class, () -> budget.deliver(BATCH, failing(failure))));
        assertEquals(List.of(), backoffs);
    }

    @Test
    void neverRetriesWhenRetriesAreDisabled() {
        RetryBudget noRetries = new RetryBudget(0, 1000, backoffs::add);

        assertThrows(VespaFeedException.class, () -> noRetries.deliver(BATCH, failing(transientFailure())));
        assertEquals(List.of(), backoffs);
    }

    @Test
    void reportsARecordAgainOnceItsBatchSucceeded() {
        ErrantRecordReporter reportingOnce = budget.reportingOnce(reporter);

        budget.deliver(BATCH, records -> reportingOnce.report(RECORD, ERROR));
        budget.deliver(BATCH, records -> reportingOnce.report(RECORD, ERROR));

        verify(reporter, times(2)).report(any(), any());
    }

    @Test
    void reportsARecordAgainOnceTheBudgetGivesUp() {
        ErrantRecordReporter reportingOnce = budget.reportingOnce(reporter);
        Consumer<Collection<SinkRecord>> reportAndFail = records -> {
            reportingOnce.report(RECORD, ERROR);
            throw fatalFailure();
        };

        assertThrows(VespaFeedException.class, () -> budget.deliver(BATCH, reportAndFail));
        assertThrows(VespaFeedException.class, () -> budget.deliver(BATCH, reportAndFail));

        verify(reporter, times(2)).report(any(), any());
    }

    @Test
    void tellsRecordsApartByTopicPartitionAndOffset() {
        ErrantRecordReporter reportingOnce = budget.reportingOnce(reporter);

        reportingOnce.report(record("topic", 0, 7), ERROR);
        reportingOnce.report(record("other", 0, 7), ERROR);
        reportingOnce.report(record("topic", 1, 7), ERROR);
        reportingOnce.report(record("topic", 0, 8), ERROR);
        reportingOnce.report(record("topic", 0, 8), ERROR);

        verify(reporter, times(4)).report(any(), any());
    }

    private static void assertRetries(Executable deliver) {
        RetriableException thrown = assertThrows(RetriableException.class, deliver);
        assertInstanceOf(VespaFeedException.class, thrown.getCause());
    }

    private static Consumer<Collection<SinkRecord>> failing(VespaFeedException failure) {
        return records -> {
            throw failure;
        };
    }

    private static VespaFeedException transientFailure() {
        return new VespaFeedException(VespaFeedException.Kind.TRANSIENT,
                new FeedException("Status 503 executing 'POST /document/v1/': unavailable"));
    }

    private static VespaFeedException fatalFailure() {
        return new VespaFeedException(VespaFeedException.Kind.FATAL,
                new FeedException("Status 401 executing 'POST /document/v1/': unauthorized"));
    }

    private static SinkRecord record(String topic, int partition, long offset) {
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, "{}", offset);
    }
}
