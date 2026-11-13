package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.converters.KeyConverter;
import com.vinted.kafka.connect.vespa.converters.ValueConverter;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class VespaUpsertFeeder implements VespaFeeder {
    private final VespaSinkConfig config;
    private final FeedClient client;
    private final OperationParameters parameters;
    private final KeyConverter keyConverter;
    private final ValueConverter valueConverter;
    private final InvalidMessageHandler invalidMessageHandler;
    private final IndexingResultHandler indexingResultHandler;

    public VespaUpsertFeeder(FeedClient client, OperationParameters parameters, ErrantRecordReporter reporter, VespaSinkConfig config) {
        this.config = config;
        this.client = client;
        this.parameters = parameters;
        this.keyConverter = new KeyConverter();
        this.valueConverter = new ValueConverter();
        this.invalidMessageHandler = new InvalidMessageHandler(config, reporter);
        this.indexingResultHandler = new IndexingResultHandler(config, reporter);
    }

    @Override
    public void feed(Collection<SinkRecord> collection) {
        List<CompletableFuture<Result>> operations = collection
                .stream()
                .flatMap(record -> invalidMessageHandler.convert(record, this::toOperation).stream())
                .collect(Collectors.toMap(x -> x.documentId, x -> x, (existing, replacement) -> replacement))
                .values()
                .stream()
                .map(this::feedSingle)
                .collect(Collectors.toList());

        FeedClient.await(operations);
    }

    @Override
    public void close() {
        client.close();
    }

    private CompletableFuture<Result> feedSingle(Operation operation) {
        return indexingResultHandler.handle(operation.record, operation.feed(client, parameters));
    }

    private Operation toOperation(SinkRecord record) {
        String key = keyConverter.convert(record);
        String value = valueConverter.convert(record);

        DocumentId documentId = DocumentId.of(getNamespace(record), getDocumentType(record), key);
        return new Operation(record, documentId, value);
    }

    private String getNamespace(SinkRecord record) {
        return config.namespace == null ? record.topic() : config.namespace;
    }

    private String getDocumentType(SinkRecord record) {
        return config.documentType == null ? record.topic() : config.documentType;
    }

    private static class Operation {
        public final SinkRecord record;
        public final DocumentId documentId;
        public final String documentJson;

        public Operation(SinkRecord record, DocumentId documentId, String documentJson) {
            this.record = record;
            this.documentId = documentId;

            if (Strings.isNullOrEmpty(documentJson)) {
                this.documentJson = null;
            } else {
                this.documentJson = String.format("{\"fields\":%s}", documentJson);
            }
        }

        public CompletableFuture<Result> feed(FeedClient client, OperationParameters parameters) {
            if (documentJson == null) {
                return client.remove(documentId, parameters.createIfNonExistent(false));
            } else {
                return client.put(documentId, documentJson, parameters);
            }
        }
    }
}
