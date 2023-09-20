package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.VespaReporter;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.converters.KeyConverter;
import com.vinted.kafka.connect.vespa.converters.ValueConverter;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VespaUpsertFeeder implements VespaFeeder {
    private static final Logger log = LoggerFactory.getLogger(VespaUpsertFeeder.class);

    private final VespaSinkConfig config;
    private final FeedClient client;
    private final OperationParameters parameters;
    private final KeyConverter keyConverter;
    private final ValueConverter valueConverter;
    private final VespaReporter reporter;
    private final VespaResultCallbackHandler resultCallbackHandler;

    public VespaUpsertFeeder(FeedClient client, OperationParameters parameters, VespaReporter reporter, VespaSinkConfig config) {
        this.config = config;
        this.client = client;
        this.parameters = parameters;
        this.keyConverter = new KeyConverter();
        this.valueConverter = new ValueConverter();
        this.reporter = reporter;
        this.resultCallbackHandler = new VespaResultCallbackHandler(log, config, reporter);
    }

    @Override
    public void feed(Collection<SinkRecord> collection) {
        List<CompletableFuture<Result>> operations = collection
                .stream()
                .flatMap(this::toOperation)
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
        return operation
                .feed(client, parameters)
                .handle((result, throwable) -> resultCallbackHandler.handle(operation.record, result, throwable));
    }

    private Stream<Operation> toOperation(SinkRecord record) {
        try {
            String key = keyConverter.convert(record);
            String value = valueConverter.convert(record);

            DocumentId documentId = DocumentId.of(getNamespace(record), getDocumentType(record), key);
            Operation operation = new Operation(record, documentId, value);

            return Stream.of(operation);
        } catch (DataException exception) {
            reporter.report(record, exception);

            log.error("Encountered invalid record {}", record, exception);

            if (config.dropInvalidMessage) {
                return Stream.empty();
            } else {
                throw new DataException(exception);
            }
        }
    }

    private String getNamespace(SinkRecord record) {
        return config.namespace.isEmpty() ? record.topic() : config.namespace;
    }

    private String getDocumentType(SinkRecord record) {
        return config.namespace.isEmpty() ? record.topic() : config.documentType;
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
                return client.remove(documentId, parameters);
            } else {
                return client.put(documentId, documentJson, parameters);
            }
        }
    }
}
