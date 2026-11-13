package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.JsonFeeder;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.converters.ValueConverter;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class VespaRawFeeder implements VespaFeeder {
    private static final Logger log = LoggerFactory.getLogger(VespaRawFeeder.class);

    private final JsonFeeder feeder;
    private final ValueConverter valueConverter;
    private final IndexingResultHandler indexingResultHandler;

    VespaRawFeeder(FeedClient client, OperationParameters parameters, ErrantRecordReporter reporter, VespaSinkConfig config) {
        JsonFeeder.Builder builder = JsonFeeder.builder(client);

        parameters.timeout().ifPresent(builder::withTimeout);
        parameters.tracelevel().ifPresent(builder::withTracelevel);
        parameters.route().ifPresent(builder::withRoute);

        this.feeder = builder.build();
        this.valueConverter = new ValueConverter();
        this.indexingResultHandler = new IndexingResultHandler(config, reporter);
    }

    @Override
    public List<CompletableFuture<Result>> feed(Collection<SinkRecord> records) {
        return records.stream().filter(this::isOperation).map(this::feedSingle).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        feeder.close();
    }

    private boolean isOperation(SinkRecord record) {
        if (record.value() != null) {
            return true;
        }

        log.debug("Skipping tombstone {}.", Records.describe(record));
        return false;
    }

    private CompletableFuture<Result> feedSingle(SinkRecord record) {
        return indexingResultHandler.handle(record, feeder.feedSingle(valueConverter.convert(record)));
    }
}
