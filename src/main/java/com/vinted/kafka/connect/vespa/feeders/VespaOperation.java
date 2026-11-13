package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.JsonFeeder;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** A Vespa operation translated from one record. */
abstract class VespaOperation {
    final SinkRecord record;
    final OperationParameters parameters;

    private VespaOperation(SinkRecord record, OperationParameters parameters) {
        this.record = record;
        this.parameters = parameters;
    }

    /** Sends the operation through the client, or through the JSON feeder for the given parameters. */
    abstract CompletableFuture<Result> submit(FeedClient client, Function<OperationParameters, JsonFeeder> jsonFeeders);

    /** An operation on a document whose id the connector built. */
    abstract static class DocumentOperation extends VespaOperation {
        final DocumentId documentId;

        private DocumentOperation(SinkRecord record, DocumentId documentId, OperationParameters parameters) {
            super(record, parameters);
            this.documentId = documentId;
        }
    }

    static final class Put extends DocumentOperation {
        final String documentJson;

        Put(SinkRecord record, DocumentId documentId, String documentJson, OperationParameters parameters) {
            super(record, documentId, parameters);
            this.documentJson = documentJson;
        }

        @Override
        CompletableFuture<Result> submit(FeedClient client, Function<OperationParameters, JsonFeeder> jsonFeeders) {
            return client.put(documentId, documentJson, parameters);
        }
    }

    static final class Remove extends DocumentOperation {
        Remove(SinkRecord record, DocumentId documentId, OperationParameters parameters) {
            super(record, documentId, parameters);
        }

        @Override
        CompletableFuture<Result> submit(FeedClient client, Function<OperationParameters, JsonFeeder> jsonFeeders) {
            return client.remove(documentId, parameters);
        }
    }

    /** An operation in Vespa's document JSON format, which carries its own id, condition and create. */
    static final class DocumentJson extends VespaOperation {
        final String json;

        DocumentJson(SinkRecord record, String json, OperationParameters parameters) {
            super(record, parameters);
            this.json = json;
        }

        @Override
        CompletableFuture<Result> submit(FeedClient client, Function<OperationParameters, JsonFeeder> jsonFeeders) {
            return jsonFeeders.apply(parameters).feedSingle(json);
        }
    }
}
