package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.JsonFeeder;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static ai.vespa.feed.client.FeedClient.CircuitBreaker.State.OPEN;

/** Sends operations through one feed client, which it closes exactly once. */
final class FeedSession implements Closeable {
    private final FeedClient client;
    private final OperationParameters documentJsonParameters;
    private JsonFeeder jsonFeeder;

    // JsonFeeder takes timeout, route and trace level when built rather than per operation.
    FeedSession(FeedClient client, OperationParameters documentJsonParameters) {
        this.client = client;
        this.documentJsonParameters = documentJsonParameters;
    }

    CompletableFuture<Result> put(DocumentId documentId, String documentJson, OperationParameters parameters) {
        return client.put(documentId, documentJson, parameters);
    }

    CompletableFuture<Result> remove(DocumentId documentId, OperationParameters parameters) {
        return client.remove(documentId, parameters);
    }

    CompletableFuture<Result> feed(String documentJson) {
        return jsonFeeder().feedSingle(documentJson);
    }

    /** True once the client's circuit breaker opened, after which it aborts every operation. */
    boolean isBroken() {
        return client.circuitBreakerState() == OPEN;
    }

    @Override
    public void close() throws IOException {
        if (jsonFeeder == null) {
            client.close();
            return;
        }

        // Closing the JSON feeder closes the client.
        jsonFeeder.close();
    }

    private JsonFeeder jsonFeeder() {
        if (jsonFeeder != null) {
            return jsonFeeder;
        }

        JsonFeeder.Builder builder = JsonFeeder.builder(client);
        documentJsonParameters.timeout().ifPresent(builder::withTimeout);
        documentJsonParameters.route().ifPresent(builder::withRoute);
        documentJsonParameters.tracelevel().ifPresent(builder::withTracelevel);
        jsonFeeder = builder.build();
        return jsonFeeder;
    }
}
