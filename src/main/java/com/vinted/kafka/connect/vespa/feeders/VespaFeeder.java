package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.JsonFeeder;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Sends batches of records through one feed client: translates each record in the operational mode under the invalid
 * message policy, sends the operations the mode keeps, and handles each indexing result. Closing it closes the client.
 */
final class VespaFeeder<T extends VespaOperation> implements Closeable {
    private final FeedClient client;
    private final RecordTranslator<T> translator;
    private final InvalidMessageHandler invalidMessageHandler;
    private final IndexingResultHandler indexingResultHandler;
    private final Map<OperationParameters, JsonFeeder> jsonFeeders = new HashMap<>();

    VespaFeeder(FeedClient client, RecordTranslator<T> translator,
                InvalidMessageHandler invalidMessageHandler, IndexingResultHandler indexingResultHandler) {
        this.client = client;
        this.translator = translator;
        this.invalidMessageHandler = invalidMessageHandler;
        this.indexingResultHandler = indexingResultHandler;
    }

    /**
     * Translates every record before sending anything, then sends the operations without waiting for them.
     *
     * @throws org.apache.kafka.connect.errors.DataException when a record is an invalid message that is not dropped
     */
    List<CompletableFuture<Result>> feed(Collection<SinkRecord> records) {
        List<T> operations = records.stream()
                .flatMap(record -> invalidMessageHandler.convert(record, translator::translate)
                        .flatMap(Function.identity())
                        .stream())
                .collect(Collectors.toList());

        return translator.compact(operations).stream().map(this::send).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        try {
            for (JsonFeeder jsonFeeder : jsonFeeders.values()) {
                jsonFeeder.close();
            }
        } finally {
            client.close();
        }
    }

    private CompletableFuture<Result> send(T operation) {
        return indexingResultHandler.handle(operation.record, operation.submit(client, this::jsonFeeder));
    }

    // JsonFeeder takes timeout, route and trace level when built rather than per operation.
    private JsonFeeder jsonFeeder(OperationParameters parameters) {
        return jsonFeeders.computeIfAbsent(parameters, this::buildJsonFeeder);
    }

    private JsonFeeder buildJsonFeeder(OperationParameters parameters) {
        JsonFeeder.Builder builder = JsonFeeder.builder(client);
        parameters.timeout().ifPresent(builder::withTimeout);
        parameters.route().ifPresent(builder::withRoute);
        parameters.tracelevel().ifPresent(builder::withTracelevel);
        return builder.build();
    }
}
