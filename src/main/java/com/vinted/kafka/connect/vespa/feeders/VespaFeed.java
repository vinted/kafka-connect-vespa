package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.MultiFeedException;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Feeds batches of records to Vespa, rebuilding the feed client when it was never built or its breaker opened. */
public class VespaFeed implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(VespaFeed.class);

    private final VespaSinkConfig config;
    private final RecordTranslator<?> translator;
    private final InvalidMessageHandler invalidMessageHandler;
    private final IndexingResultHandler indexingResultHandler;
    private final Function<VespaSinkConfig, FeedClient> clients;
    private FeedSession session;

    private VespaFeed(VespaSinkConfig config, ErrantRecordReporter reporter, Function<VespaSinkConfig, FeedClient> clients) {
        this.config = config;
        this.translator = RecordTranslator.of(config);
        this.invalidMessageHandler = new InvalidMessageHandler(config, reporter);
        this.indexingResultHandler = new IndexingResultHandler(config, reporter);
        this.clients = clients;
    }

    /** Opens even when Vespa is unreachable; the client is then built when feeding. */
    public static VespaFeed open(VespaSinkConfig config, ErrantRecordReporter reporter, Function<VespaSinkConfig, FeedClient> clients) {
        VespaFeed feed = new VespaFeed(config, reporter, clients);

        try {
            feed.session();
        } catch (RuntimeException e) {
            if (IndexingFailure.of(e) != IndexingFailure.TRANSIENT) {
                throw new VespaFeedException(e);
            }
            log.warn("Could not connect to Vespa, will retry when feeding records.", e);
        }

        return feed;
    }

    /** @throws VespaFeedException caused by a {@link MultiFeedException}, a {@code DataException} or a client build failure */
    public void feed(Collection<SinkRecord> records) {
        try {
            FeedSession session = session();
            await(translate(translator, records).stream()
                    .map(operation -> indexingResultHandler.handle(operation.record, operation.submit(session)))
                    .collect(Collectors.toList()));
        } catch (RuntimeException e) {
            throw new VespaFeedException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (session == null) {
            return;
        }

        try {
            session.close();
        } finally {
            session = null;
        }
    }

    private FeedSession session() {
        if (session != null && !session.isBroken()) {
            return session;
        }

        if (session != null) {
            log.warn("Vespa feed client circuit breaker is open, recreating the feed client.");
            closeQuietly(session);
            session = null;
        }

        session = new FeedSession(clients.apply(config), RecordTranslator.commonParameters(config));
        return session;
    }

    /** @throws org.apache.kafka.connect.errors.DataException when a record is an invalid message that is not dropped */
    private <T extends VespaOperation> List<T> translate(RecordTranslator<T> translator, Collection<SinkRecord> records) {
        List<T> operations = records.stream()
                .flatMap(record -> invalidMessageHandler.convert(record, translator::translate)
                        .flatMap(Function.identity())
                        .stream())
                .collect(Collectors.toList());

        return translator.compact(operations);
    }

    private static void closeQuietly(FeedSession session) {
        try {
            session.close();
        } catch (Exception e) {
            log.warn("Error closing the Vespa feed client.", e);
        }
    }

    private static void await(List<CompletableFuture<Result>> operations) {
        List<FeedException> failures = operations.stream()
                .map(VespaFeed::failure)
                .flatMap(Optional::stream)
                .collect(Collectors.toList());

        if (!failures.isEmpty()) {
            throw new MultiFeedException(failures);
        }
    }

    private static Optional<FeedException> failure(CompletableFuture<Result> operation) {
        Throwable failure = operation.handle((result, throwable) -> throwable).join();
        return Optional.ofNullable(failure).map(VespaFeed::asFeedException);
    }

    private static FeedException asFeedException(Throwable throwable) {
        return throwable instanceof FeedException ? (FeedException) throwable : new FeedException(throwable);
    }
}
