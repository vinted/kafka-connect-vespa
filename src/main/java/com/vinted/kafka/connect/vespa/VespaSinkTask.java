package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.OperationParameters;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.vinted.kafka.connect.vespa.factories.VespaFeedClientFactory;
import com.vinted.kafka.connect.vespa.factories.VespaOperationParametersFactory;
import com.vinted.kafka.connect.vespa.feeders.VespaFeeder;
import com.vinted.kafka.connect.vespa.feeders.VespaRawFeeder;
import com.vinted.kafka.connect.vespa.feeders.VespaUpsertFeeder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class VespaSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(VespaSinkTask.class);

    private VespaSinkConfig config;
    private OperationParameters parameters;
    private FeedClient client;
    private VespaFeeder feeder;
    private VespaReporter reporter;

    @Override
    public void start(Map<String, String> props) {
        start(props, null);
    }

    protected void start(Map<String, String> props, FeedClient client) {
        log.info("Starting Vespa Sink Task.");

        try {
            reporter = new VespaReporter(context.errantRecordReporter());
        } catch (NullPointerException | NoSuchMethodError | NoClassDefFoundError e) {
            log.warn("Could not get errant record reporter, will not report bad records.", e);
            reporter = new VespaReporter(null);
        }

        this.config = new VespaSinkConfig(props);
        this.client = client != null ? client : VespaFeedClientFactory.create(config);
        this.parameters = VespaOperationParametersFactory.create(config);

        switch (config.operationalMode) {
            case UPSERT:
                feeder = new VespaUpsertFeeder(this.client, parameters, reporter, config);
                break;
            case RAW:
                feeder = new VespaRawFeeder(this.client, parameters, reporter, config);
                break;
            default:
                throw new ConnectException(String.format("Unknown operational mode: %s", config.operationalMode));
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        log.debug("Putting {} records.", collection.size());

        feeder.feed(collection);
    }

    @Override
    public void stop() {
        log.info("Stopping Vespa Sink Task.");

        try {
            feeder.close();
        } catch (IOException e) {
            log.error("Error closing Vespa feeder.", e);
            throw new ConnectException(e);
        }
    }

    @Override
    public String version() {
        return VersionUtil.version(this.getClass());
    }
}
