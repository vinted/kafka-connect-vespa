package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.JsonFeeder;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaReporter;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.converters.ValueConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class VespaRawFeeder implements VespaFeeder {
    private static final Logger log = LoggerFactory.getLogger(VespaRawFeeder.class);

    private final JsonFeeder feeder;
    private final ValueConverter valueConverter;
    private final VespaFeederHandler vespaFeederHandler;

    public VespaRawFeeder(FeedClient client, OperationParameters parameters, VespaReporter reporter, VespaSinkConfig config) {
        JsonFeeder.Builder builder = JsonFeeder.builder(client);

        parameters.timeout().ifPresent(builder::withTimeout);
        parameters.tracelevel().ifPresent(builder::withTracelevel);
        parameters.route().ifPresent(builder::withRoute);

        this.feeder = builder.build();
        this.valueConverter = new ValueConverter();
        this.vespaFeederHandler = new VespaFeederHandler(log, config, reporter);
    }

    @Override
    public void feed(Collection<SinkRecord> collection) {
        FeedClient.await(collection.stream().map(this::feedSingle).collect(Collectors.toList()));
    }

    @Override
    public void close() throws IOException {
        feeder.close();
    }

    private CompletableFuture<Result> feedSingle(SinkRecord record) {
        return vespaFeederHandler.handle(record, feeder.feedSingle(valueConverter.convert(record)));
    }
}
