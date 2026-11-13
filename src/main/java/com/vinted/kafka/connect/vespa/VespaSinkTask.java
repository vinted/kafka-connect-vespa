package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedClient;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.vinted.kafka.connect.vespa.feeders.VespaFeed;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class VespaSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(VespaSinkTask.class);
    private static final ErrantRecordReporter NO_REPORTER = (record, error) -> CompletableFuture.completedFuture(null);

    private final Function<VespaSinkConfig, FeedClient> clients;
    private VespaFeed feed;
    private RetryBudget retryBudget;

    public VespaSinkTask() {
        this(VespaSinkConfig::feedClient);
    }

    VespaSinkTask(Function<VespaSinkConfig, FeedClient> clients) {
        this.clients = clients;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Vespa Sink Task.");

        ErrantRecordReporter reporter = errantRecordReporter();
        VespaSinkConfig config = new VespaSinkConfig(props);
        this.retryBudget = config.retryBudget(context::timeout);
        log.info("Using {} operational mode.", config.operationalMode());

        this.feed = VespaFeed.open(config, retryBudget.reportingOnce(reporter), clients);
    }

    private ErrantRecordReporter errantRecordReporter() {
        try {
            // Null unless errors.deadletterqueue.topic.name or errors.log.enable is set.
            ErrantRecordReporter reporter = context.errantRecordReporter();
            if (reporter == null) {
                log.info("Errant record reporter not configured.");
                return NO_REPORTER;
            }
            return reporter;
        } catch (NoClassDefFoundError | NoSuchMethodError e) {
            // Will occur in Connect runtimes earlier than 2.6
            log.warn("Kafka versions prior to 2.6 do not support the errant record reporter.");
            return NO_REPORTER;
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }

        log.debug("Putting {} records.", collection.size());
        retryBudget.deliver(collection, feed::feed);
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
