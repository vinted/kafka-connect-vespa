package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.OperationStats;
import ai.vespa.feed.client.Result;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** A feed client that succeeds every operation at once, in memory, to measure what the connector itself costs. */
final class AcknowledgingFeedClient implements FeedClient {
    private static final CompletableFuture<Result> SUCCESS = CompletableFuture.completedFuture(new Result() {
        @Override
        public Type type() {
            return Type.success;
        }

        @Override
        public DocumentId documentId() {
            return null;
        }

        @Override
        public Optional<String> resultMessage() {
            return Optional.empty();
        }

        @Override
        public Optional<String> traceMessage() {
            return Optional.empty();
        }
    });

    @Override
    public CompletableFuture<Result> put(DocumentId documentId, String documentJson, OperationParameters params) {
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Result> update(DocumentId documentId, String updateJson, OperationParameters params) {
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Result> remove(DocumentId documentId, OperationParameters params) {
        return SUCCESS;
    }

    @Override
    public OperationStats stats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resetStats() {
    }

    @Override
    public CircuitBreaker.State circuitBreakerState() {
        return CircuitBreaker.State.CLOSED;
    }

    @Override
    public void close(boolean graceful) {
    }
}
