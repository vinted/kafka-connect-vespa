package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Failure;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Kind;
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
    private final IndexingResultHandler indexingResultHandler;
    private final Function<VespaSinkConfig, FeedClient> clients;
    private FeedSession session;

    private VespaFeed(VespaSinkConfig config, ErrantRecordReporter reporter, Function<VespaSinkConfig, FeedClient> clients) {
        this.config = config;
        this.translator = RecordTranslator.of(config, reporter);
        this.indexingResultHandler = new IndexingResultHandler(config, reporter);
        this.clients = clients;
    }

    /** Opens even when Vespa is unreachable; the client is then built when feeding. */
    public static VespaFeed open(VespaSinkConfig config, ErrantRecordReporter reporter, Function<VespaSinkConfig, FeedClient> clients) {
        VespaFeed feed = new VespaFeed(config, reporter, clients);

        try {
            feed.session();
        } catch (RuntimeException e) {
            IndexingFailure failure = IndexingFailure.of(e);
            if (failure != IndexingFailure.TRANSIENT) {
                throw new VespaFeedException(Kind.of(failure), e);
            }
            log.warn("Could not connect to Vespa, will retry when feeding records.", e);
        }

        return feed;
    }

    public void feed(Collection<SinkRecord> records) {
        List<? extends VespaOperation> operations = translator.operations(records);
        List<Failure> failures = submit(operations).stream()
                .map(CompletableFuture::join)
                .flatMap(Optional::stream)
                .collect(Collectors.toList());

        if (!failures.isEmpty()) {
            throw new VespaFeedException(failures);
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

    private List<CompletableFuture<Optional<Failure>>> submit(List<? extends VespaOperation> operations) {
        try {
            FeedSession session = session();
            return operations.stream()
                    .map(operation -> indexingResultHandler.handle(operation.record, operation.submit(session)))
                    .collect(Collectors.toList());
        } catch (RuntimeException e) {
            throw new VespaFeedException(Kind.of(IndexingFailure.of(e)), e);
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

        session = new FeedSession(clients.apply(config), config.operationParameters());
        return session;
    }

    private static void closeQuietly(FeedSession session) {
        try {
            session.close();
        } catch (Exception e) {
            log.warn("Error closing the Vespa feed client.", e);
        }
    }
}
