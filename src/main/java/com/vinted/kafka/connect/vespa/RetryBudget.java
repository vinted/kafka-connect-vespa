package com.vinted.kafka.connect.vespa;

import com.vinted.kafka.connect.vespa.feeders.RecordCoordinates;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Delivers batches, asking Connect to redeliver a batch that failed only with transient failures while the budget
 * lasts, and reports each record once across the redeliveries of its batch.
 */
final class RetryBudget {
    private static final Logger log = LoggerFactory.getLogger(RetryBudget.class);

    private final int maxRetries;
    private final long initialBackoffMs;
    private final LongConsumer backOff;
    // Written from feed client callback threads as well as the task thread.
    private final Set<RecordCoordinates> reported = ConcurrentHashMap.newKeySet();
    private int attempt;

    /**
     * @param backOff asks Connect to wait this many milliseconds before redelivering the batch
     */
    RetryBudget(int maxRetries, long initialBackoffMs, LongConsumer backOff) {
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.backOff = backOff;
    }

    /** Reports each record once across the redeliveries of its batch. */
    ErrantRecordReporter reportingOnce(ErrantRecordReporter reporter) {
        return (record, error) -> reported.add(RecordCoordinates.of(record))
                ? reporter.report(record, error)
                : CompletableFuture.completedFuture(null);
    }

    /**
     * Feeds the records.
     *
     * @throws RetriableException after asking Connect to back off, when the feed failed only transiently and the
     *                            budget lasts
     * @throws VespaFeedException when the feed failed otherwise, which fails the task
     */
    void deliver(Collection<SinkRecord> records, Consumer<Collection<SinkRecord>> feed) {
        try {
            feed.accept(records);
        } catch (VespaFeedException e) {
            throw failed(e, records.size());
        }

        reset();
    }

    private ConnectException failed(VespaFeedException e, int records) {
        if (!e.isTransient()) {
            reset();
            log.error("Failing the task after failing to feed {} records to Vespa: {}", records, e.getMessage());
            return e;
        }

        if (attempt >= maxRetries) {
            reset();
            log.error("Vespa is still unavailable after {} retries: {}. To wait longer, increase '{}' or '{}'.",
                    maxRetries, e.getMessage(), VespaSinkConfig.MAX_RETRIES_CONFIG, VespaSinkConfig.RETRY_BACKOFF_MS_CONFIG);
            return e;
        }

        attempt++;
        long backoffMs = Backoff.randomBackoffMs(attempt, initialBackoffMs);
        log.warn("Vespa is unavailable, retrying {} records in {} ms (retry {} of {}): {}",
                records, backoffMs, attempt, maxRetries, e.getMessage());
        backOff.accept(backoffMs);
        return new RetriableException(e);
    }

    private void reset() {
        attempt = 0;
        reported.clear();
    }
}
