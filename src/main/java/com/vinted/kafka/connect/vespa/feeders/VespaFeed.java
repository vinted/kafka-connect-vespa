package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.MultiFeedException;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.factories.VespaOperationParametersFactory;
import org.apache.kafka.connect.errors.ConnectException;
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

import static ai.vespa.feed.client.FeedClient.CircuitBreaker.State.OPEN;

/** Feeds batches of records to Vespa, rebuilding the feed client when it was never built or its breaker opened. */
public class VespaFeed implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(VespaFeed.class);

    private final VespaSinkConfig config;
    private final OperationParameters parameters;
    private final ErrantRecordReporter reporter;
    private final Function<VespaSinkConfig, FeedClient> clients;
    private FeedClient client;
    private VespaFeeder feeder;

    private VespaFeed(VespaSinkConfig config, ErrantRecordReporter reporter, Function<VespaSinkConfig, FeedClient> clients) {
        this.config = config;
        this.parameters = VespaOperationParametersFactory.create(config);
        this.reporter = reporter;
        this.clients = clients;
    }

    /** Opens even when Vespa is unreachable; the client is then built when feeding. */
    public static VespaFeed open(VespaSinkConfig config, ErrantRecordReporter reporter, Function<VespaSinkConfig, FeedClient> clients) {
        VespaFeed feed = new VespaFeed(config, reporter, clients);

        try {
            feed.feeder();
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
            await(feeder().feed(records));
        } catch (RuntimeException e) {
            throw new VespaFeedException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (feeder == null) {
            return;
        }

        try {
            feeder.close();
        } finally {
            feeder = null;
            client = null;
        }
    }

    private VespaFeeder feeder() {
        if (feeder != null && client.circuitBreakerState() != OPEN) {
            return feeder;
        }

        if (feeder != null) {
            log.warn("Vespa feed client circuit breaker is open, recreating the feed client.");
            closeQuietly(feeder);
            feeder = null;
        }

        client = clients.apply(config);
        feeder = createFeeder(client);
        return feeder;
    }

    private VespaFeeder createFeeder(FeedClient client) {
        switch (config.operationalMode) {
            case UPSERT:
                return new VespaUpsertFeeder(client, parameters, reporter, config);
            case RAW:
                return new VespaRawFeeder(client, parameters, reporter, config);
            default:
                throw new ConnectException(String.format("Unknown operational mode: %s", config.operationalMode));
        }
    }

    private static void closeQuietly(VespaFeeder feeder) {
        try {
            feeder.close();
        } catch (Exception e) {
            log.warn("Error closing the Vespa feeder.", e);
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
