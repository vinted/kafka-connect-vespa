package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Failure;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Feeds batches of records to Vespa: translates them in the operational mode, handles their indexing results, and
 * asks Connect to redeliver a batch that failed only transiently while the retry budget lasts, reporting each record
 * once across the redeliveries of its batch.
 */
public class VespaFeed implements Closeable {
    private final RetryBudget retryBudget;
    private final RecordTranslator<?> translator;
    private final IndexingResultHandler indexingResultHandler;
    private final FeedSession session;

    private VespaFeed(RetryBudget retryBudget, RecordTranslator<?> translator,
                      IndexingResultHandler indexingResultHandler, FeedSession session) {
        this.retryBudget = retryBudget;
        this.translator = translator;
        this.indexingResultHandler = indexingResultHandler;
        this.session = session;
    }

    /**
     * Opens even when Vespa is unreachable; the client is then built when feeding.
     *
     * @param backOff asks Connect to wait this many milliseconds before redelivering a batch
     * @throws VespaFeedException when the client fails to build for a reason other than a transient failure
     */
    public static VespaFeed open(VespaSinkConfig config, ErrantRecordReporter reporter, Supplier<FeedClient> clients,
                                 LongConsumer backOff) {
        RetryBudget retryBudget = new RetryBudget(config.maxRetries(), config.retryBackoffMs(), backOff);
        ErrantRecordReporter reportingOnce = retryBudget.reportingOnce(reporter);
        return new VespaFeed(
                retryBudget,
                RecordTranslator.of(config, reportingOnce),
                new IndexingResultHandler(config.behaviorOnMalformedDoc(), reportingOnce),
                FeedSession.open(clients, config.operationParameters()));
    }

    /**
     * Feeds the records.
     *
     * @throws org.apache.kafka.connect.errors.RetriableException after asking Connect to back off, when the feed
     *                                                            failed only transiently and the budget lasts
     * @throws VespaFeedException                                 when the feed failed otherwise, which fails the task
     */
    public void deliver(Collection<SinkRecord> records) {
        retryBudget.deliver(records, this::feed);
    }

    /** Feeds the records once, without retrying. */
    void feed(Collection<SinkRecord> records) {
        List<? extends VespaOperation> operations = translator.operations(records);
        List<CompletableFuture<Result>> results = session.submit(operations);
        List<CompletableFuture<Optional<Failure>>> outcomes = IntStream.range(0, operations.size())
                .mapToObj(i -> indexingResultHandler.handle(operations.get(i).record, results.get(i)))
                .collect(Collectors.toList());

        List<Failure> failures = outcomes.stream()
                .map(CompletableFuture::join)
                .flatMap(Optional::stream)
                .collect(Collectors.toList());

        if (!failures.isEmpty()) {
            throw new VespaFeedException(failures);
        }
    }

    @Override
    public void close() throws IOException {
        session.close();
    }
}
