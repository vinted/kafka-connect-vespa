package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.OperationParameters;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.vinted.kafka.connect.vespa.factories.VespaFeedClientFactory;
import com.vinted.kafka.connect.vespa.factories.VespaOperationParametersFactory;
import com.vinted.kafka.connect.vespa.feeders.IndexingFailure;
import com.vinted.kafka.connect.vespa.feeders.VespaFeeder;
import com.vinted.kafka.connect.vespa.feeders.VespaRawFeeder;
import com.vinted.kafka.connect.vespa.feeders.VespaUpsertFeeder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import static ai.vespa.feed.client.FeedClient.CircuitBreaker.State.OPEN;

public class VespaSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(VespaSinkTask.class);

    private VespaSinkConfig config;
    private OperationParameters parameters;
    private ErrantRecordReporter reporter;
    private Function<VespaSinkConfig, FeedClient> clientFactory;
    private FeedClient client;
    private VespaFeeder feeder;
    private int retryAttempt;

    @Override
    public void start(Map<String, String> props) {
        start(props, VespaFeedClientFactory::create);
    }

    protected void start(Map<String, String> props, FeedClient client) {
        start(props, config -> client);
    }

    protected void start(Map<String, String> props, Function<VespaSinkConfig, FeedClient> clientFactory) {
        log.info("Starting Vespa Sink Task.");

        reporter = null;
        try {
            // Null unless errors.deadletterqueue.topic.name or errors.log.enable is set.
            reporter = context.errantRecordReporter();
            if (reporter == null) {
                log.info("Errant record reporter not configured.");
            }
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            // Will occur in Connect runtimes earlier than 2.6
            log.warn("Kafka versions prior to 2.6 do not support the errant record reporter.");
        }

        this.config = new VespaSinkConfig(props);
        this.parameters = VespaOperationParametersFactory.create(config);
        this.clientFactory = clientFactory;
        this.retryAttempt = 0;
        log.info("Using {} operational mode.", config.operationalMode);

        try {
            feeder();
        } catch (FeedException e) {
            // Building the client makes a handshake with Vespa. Like the Elasticsearch sink, start even when the
            // cluster is unreachable, e.g. mid-upgrade; feeding retries building it.
            if (IndexingFailure.of(e) != IndexingFailure.TRANSIENT) {
                throw e;
            }
            log.warn("Could not connect to Vespa, will retry when feeding records.", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }

        log.debug("Putting {} records.", collection.size());

        try {
            feeder().feed(collection);
            retryAttempt = 0;
        } catch (Exception e) {
            throw failure(e, collection.size());
        }
    }

    /**
     * Returns a {@link RetriableException} for a transient failure while the retry budget lasts, after asking Connect
     * to back off; otherwise a {@link ConnectException} that fails the task.
     */
    private ConnectException failure(Exception e, int records) {
        boolean isTransient = IndexingFailure.of(e) == IndexingFailure.TRANSIENT;

        if (isTransient && retryAttempt < config.maxRetries) {
            retryAttempt++;
            long backoffMs = Backoff.randomBackoffMs(retryAttempt, config.retryBackoffMs);
            log.warn("Vespa is unavailable, retrying {} records in {} ms (retry {} of {}).",
                    records, backoffMs, retryAttempt, config.maxRetries, e);
            context.timeout(backoffMs);
            return new RetriableException(e);
        }

        if (isTransient) {
            log.error("Vespa is still unavailable after {} retries. To wait longer, increase '{}' or '{}'.",
                    config.maxRetries, VespaSinkConfig.MAX_RETRIES_CONFIG, VespaSinkConfig.RETRY_BACKOFF_MS_CONFIG, e);
            return new ConnectException(e);
        }

        log.error("Error feeding records to Vespa.", e);
        return new ConnectException(e);
    }

    @Override
    public void stop() {
        log.info("Stopping Vespa Sink Task.");

        if (feeder == null) {
            return;
        }

        try {
            feeder.close();
        } catch (IOException e) {
            log.error("Error closing Vespa feeder.", e);
            throw new ConnectException(e);
        } finally {
            feeder = null;
        }
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }

    /**
     * Returns the feeder, building the feed client when it was never built or is dead: once its circuit breaker
     * opens, the client fails every operation with "Operation aborted" and cannot recover.
     */
    private VespaFeeder feeder() {
        if (feeder != null && client.circuitBreakerState() != OPEN) {
            return feeder;
        }

        if (feeder != null) {
            log.warn("Vespa feed client circuit breaker is open, recreating the feed client.");
            closeQuietly(feeder);
            feeder = null;
        }

        client = clientFactory.apply(config);
        feeder = createFeeder(client);
        return feeder;
    }

    private static void closeQuietly(VespaFeeder feeder) {
        try {
            feeder.close();
        } catch (Exception e) {
            log.warn("Error closing the Vespa feeder.", e);
        }
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
}
