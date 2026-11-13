package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedClient;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.vinted.kafka.connect.vespa.factories.VespaFeedClientFactory;
import com.vinted.kafka.connect.vespa.feeders.IndexingFailure;
import com.vinted.kafka.connect.vespa.feeders.VespaFeed;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException;
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

public class VespaSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(VespaSinkTask.class);

    private final Function<VespaSinkConfig, FeedClient> clients;
    private VespaSinkConfig config;
    private VespaFeed feed;
    private int retryAttempt;

    public VespaSinkTask() {
        this(VespaFeedClientFactory::create);
    }

    VespaSinkTask(Function<VespaSinkConfig, FeedClient> clients) {
        this.clients = clients;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Vespa Sink Task.");

        ErrantRecordReporter reporter = errantRecordReporter();
        this.config = new VespaSinkConfig(props);
        this.retryAttempt = 0;
        log.info("Using {} operational mode.", config.operationalMode);

        this.feed = VespaFeed.open(config, reporter, clients);
    }

    private ErrantRecordReporter errantRecordReporter() {
        try {
            // Null unless errors.deadletterqueue.topic.name or errors.log.enable is set.
            ErrantRecordReporter reporter = context.errantRecordReporter();
            if (reporter == null) {
                log.info("Errant record reporter not configured.");
            }
            return reporter;
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            // Will occur in Connect runtimes earlier than 2.6
            log.warn("Kafka versions prior to 2.6 do not support the errant record reporter.");
            return null;
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }

        log.debug("Putting {} records.", collection.size());

        try {
            feed.feed(collection);
            retryAttempt = 0;
        } catch (VespaFeedException e) {
            throw failure(e, collection.size());
        }
    }

    /**
     * Returns a {@link RetriableException} for a transient failure while the retry budget lasts, after asking Connect
     * to back off; otherwise a {@link ConnectException} that fails the task.
     */
    private ConnectException failure(VespaFeedException e, int records) {
        if (e.failure() != IndexingFailure.TRANSIENT) {
            log.error("Error feeding records to Vespa.", e.getCause());
            return e;
        }

        if (retryAttempt >= config.maxRetries) {
            log.error("Vespa is still unavailable after {} retries. To wait longer, increase '{}' or '{}'.",
                    config.maxRetries, VespaSinkConfig.MAX_RETRIES_CONFIG, VespaSinkConfig.RETRY_BACKOFF_MS_CONFIG,
                    e.getCause());
            return e;
        }

        retryAttempt++;
        long backoffMs = Backoff.randomBackoffMs(retryAttempt, config.retryBackoffMs);
        log.warn("Vespa is unavailable, retrying {} records in {} ms (retry {} of {}).",
                records, backoffMs, retryAttempt, config.maxRetries, e.getCause());
        context.timeout(backoffMs);
        return new RetriableException(e.getCause());
    }

    @Override
    public void stop() {
        log.info("Stopping Vespa Sink Task.");

        if (feed == null) {
            return;
        }

        try {
            feed.close();
        } catch (IOException e) {
            log.error("Error closing Vespa feeder.", e);
            throw new ConnectException(e);
        } finally {
            feed = null;
        }
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
