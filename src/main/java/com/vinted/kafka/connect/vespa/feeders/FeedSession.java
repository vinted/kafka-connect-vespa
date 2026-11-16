package com.vinted.kafka.connect.vespa.feeders;

import static ai.vespa.feed.client.FeedClient.CircuitBreaker.State.OPEN;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.JsonFeeder;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps a working feed client: builds it when opened, or when submitting while Vespa was
 * unreachable, and replaces it once its circuit breaker opened. Closing the session closes the
 * client exactly once.
 */
final class FeedSession implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(FeedSession.class);

  private final Supplier<FeedClient> clients;
  private Client client;

  private FeedSession(Supplier<FeedClient> clients) {
    this.clients = clients;
  }

  /**
   * Opens even when Vespa is unreachable; the client is then built when submitting.
   *
   * @throws VespaFeedException when the client fails to build for a reason other than a transient
   *     failure
   */
  static FeedSession open(Supplier<FeedClient> clients) {
    FeedSession session = new FeedSession(clients);

    try {
      session.client();
    } catch (VespaFeedException e) {
      if (!e.isTransient()) {
        throw e;
      }
      log.warn("Could not connect to Vespa, will retry when feeding records.", e.getCause());
    }

    return session;
  }

  /**
   * Submits the operations through one working client, returning their results in order. An
   * operation the client refuses fails its result. Every document JSON operation in a session has
   * the same parameters, as the client's JSON feeder takes them when built rather than per
   * operation.
   *
   * @throws VespaFeedException when the client fails to build
   */
  List<CompletableFuture<Result>> submit(List<? extends VespaOperation> operations) {
    Client client = client();
    return operations.stream()
        .map(operation -> send(operation, client))
        .collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    if (client == null) {
      return;
    }

    try {
      client.close();
    } finally {
      client = null;
    }
  }

  private static CompletableFuture<Result> send(VespaOperation operation, Client client) {
    try {
      return operation.submit(client);
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private Client client() {
    if (client != null && !client.isBroken()) {
      return client;
    }

    if (client != null) {
      log.warn("Vespa feed client circuit breaker is open, recreating the feed client.");
      closeQuietly(client);
      client = null;
    }

    client = new Client(build());
    return client;
  }

  private FeedClient build() {
    try {
      return clients.get();
    } catch (RuntimeException e) {
      throw new VespaFeedException(IndexingFailure.of(e).kind(), e);
    }
  }

  private static void closeQuietly(Client client) {
    try {
      client.close();
    } catch (Exception e) {
      log.warn("Error closing the Vespa feed client.", e);
    }
  }

  /** One feed client, and the JSON feeder built on it when first needed. */
  static final class Client implements Closeable {
    private final FeedClient client;
    private JsonFeeder jsonFeeder;

    private Client(FeedClient client) {
      this.client = client;
    }

    CompletableFuture<Result> put(
        DocumentId documentId, String documentJson, OperationParameters parameters) {
      return client.put(documentId, documentJson, parameters);
    }

    CompletableFuture<Result> remove(DocumentId documentId, OperationParameters parameters) {
      return client.remove(documentId, parameters);
    }

    CompletableFuture<Result> feed(String documentJson, OperationParameters parameters) {
      return jsonFeeder(parameters).feedSingle(documentJson);
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

    private boolean isBroken() {
      return client.circuitBreakerState() == OPEN;
    }

    private JsonFeeder jsonFeeder(OperationParameters parameters) {
      if (jsonFeeder != null) {
        return jsonFeeder;
      }

      JsonFeeder.Builder builder = JsonFeeder.builder(client);
      parameters.timeout().ifPresent(builder::withTimeout);
      parameters.route().ifPresent(builder::withRoute);
      parameters.tracelevel().ifPresent(builder::withTracelevel);
      jsonFeeder = builder.build();
      return jsonFeeder;
    }
  }
}
