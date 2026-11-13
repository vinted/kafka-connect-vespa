package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.JsonFeeder;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.converters.ValueConverter;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class VespaRawFeeder implements VespaFeeder {
    private final JsonFeeder feeder;
    private final ValueConverter valueConverter;
    private final IndexingResultHandler indexingResultHandler;

    public VespaRawFeeder(FeedClient client, OperationParameters parameters, ErrantRecordReporter reporter, VespaSinkConfig config) {
        JsonFeeder.Builder builder = JsonFeeder.builder(client);

        parameters.timeout().ifPresent(builder::withTimeout);
        parameters.tracelevel().ifPresent(builder::withTracelevel);
        parameters.route().ifPresent(builder::withRoute);

        this.feeder = builder.build();
        this.valueConverter = new ValueConverter();
        this.indexingResultHandler = new IndexingResultHandler(config, reporter);
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
        return indexingResultHandler.handle(record, feeder.feedSingle(valueConverter.convert(record)));
    }
}
