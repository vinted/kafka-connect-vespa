package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.concurrent.CompletableFuture;

/** A Vespa operation translated from one record. */
abstract class VespaOperation {
    final SinkRecord record;

    private VespaOperation(SinkRecord record) {
        this.record = record;
    }

    abstract CompletableFuture<Result> submit(FeedSession session);

    /** An operation on a document whose id the connector built. */
    abstract static class DocumentOperation extends VespaOperation {
        final DocumentId documentId;
        final OperationParameters parameters;

        private DocumentOperation(SinkRecord record, DocumentId documentId, OperationParameters parameters) {
            super(record);
            this.documentId = documentId;
            this.parameters = parameters;
        }
    }

    static final class Put extends DocumentOperation {
        final String documentJson;

        Put(SinkRecord record, DocumentId documentId, String documentJson, OperationParameters parameters) {
            super(record, documentId, parameters);
            this.documentJson = documentJson;
        }

        @Override
        CompletableFuture<Result> submit(FeedSession session) {
            return session.put(documentId, documentJson, parameters);
        }
    }

    static final class Remove extends DocumentOperation {
        Remove(SinkRecord record, DocumentId documentId, OperationParameters parameters) {
            super(record, documentId, parameters);
        }

        @Override
        CompletableFuture<Result> submit(FeedSession session) {
            return session.remove(documentId, parameters);
        }
    }

    /** An operation in Vespa's document JSON format, which carries its own id, condition and create. */
    static final class DocumentJson extends VespaOperation {
        final String json;

        DocumentJson(SinkRecord record, String json) {
            super(record);
            this.json = json;
        }

        @Override
        CompletableFuture<Result> submit(FeedSession session) {
            return session.feed(json);
        }
    }
}
