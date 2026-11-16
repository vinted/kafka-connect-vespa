package com.vinted.kafka.connect.vespa.feeders;

import static com.vinted.kafka.connect.vespa.mocks.FakeFeedClient.Answer.refusal;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertNothingSent;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertSent;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentJson;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Put;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Remove;
import com.vinted.kafka.connect.vespa.mocks.FakeFeedClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class FeedSessionTest {
  private static final DocumentId ID = DocumentId.of("id:ns:dt::a");
  private static final String DOCUMENT_JSON = "{\"put\":\"id:ns:dt::a\",\"fields\":{}}";
  private static final SinkRecord RECORD =
      new SinkRecord("topic", 0, null, "a", Schema.STRING_SCHEMA, "{}", 7);
  private static final FeedException UNAUTHORIZED =
      new FeedException("Status 401 executing 'POST /document/v1/': unauthorized");
  private static final FeedException HANDSHAKE_FAILED =
      new FeedException(
          "failed handshake with server after PT30S",
          new java.net.ConnectException("Connection refused"));

  private final FakeFeedClient client = new FakeFeedClient();

  @Test
  void closesTheClientOnceWhenOnlyDocumentOperationsWereSent() throws IOException {
    FeedSession session = FeedSession.open(() -> client);
    join(session.submit(List.of(put(), new Remove(RECORD, ID, OperationParameters.empty()))));

    session.close();

    assertEquals(1, client.closes());
  }

  @Test
  void closesTheClientOnceAfterSendingDocumentJsonOperations() throws IOException {
    FeedSession session = FeedSession.open(() -> client);
    join(session.submit(List.of(documentJson(), documentJson())));

    session.close();

    assertEquals(1, client.closes());
  }

  @Test
  void replacesTheClientOnceItsCircuitBreakerOpens() {
    FakeFeedClient replacement = new FakeFeedClient();
    Clients clients = new Clients(client, replacement);
    FeedSession session = FeedSession.open(clients);

    client.circuitBreakerState(FeedClient.CircuitBreaker.State.HALF_OPEN);
    join(session.submit(List.of(put())));
    client.circuitBreakerState(FeedClient.CircuitBreaker.State.OPEN);
    join(session.submit(List.of(put())));

    assertTrue(client.isClosed());
    assertEquals(2, clients.built);
    assertSent(replacement, "put id:ns:dt::a {\"fields\":{}}");
  }

  @Test
  void opensWhileVespaIsUnreachableAndConnectsWhenSubmitting() {
    Clients clients = new Clients(HANDSHAKE_FAILED, HANDSHAKE_FAILED, client);

    FeedSession session = assertDoesNotThrow(() -> FeedSession.open(clients));
    VespaFeedException thrown =
        assertThrows(VespaFeedException.class, () -> session.submit(List.of(put())));
    join(session.submit(List.of(put())));

    assertTrue(thrown.isTransient());
    assertEquals(3, clients.built);
    assertSent(client, "put id:ns:dt::a {\"fields\":{}}");
  }

  @Test
  void submitsNoOperationsWithoutBuildingClient() {
    Clients clients = new Clients(HANDSHAKE_FAILED);
    FeedSession session = FeedSession.open(clients);

    assertEquals(List.of(), session.submit(List.of()));
    assertEquals(1, clients.built);
  }

  @Test
  void failsToOpenOnOtherClientErrors() {
    UncheckedIOException missingCertificate =
        new UncheckedIOException(new NoSuchFileException("client.pem"));

    VespaFeedException thrown =
        assertThrows(
            VespaFeedException.class, () -> FeedSession.open(new Clients(missingCertificate)));

    assertSame(missingCertificate, thrown.getCause());
    assertEquals("fatal failure: NoSuchFileException: client.pem", thrown.getMessage());
  }

  @Test
  void failsToOpenWhenTheHandshakeFailsForNonTransientReason() {
    VespaFeedException thrown =
        assertThrows(VespaFeedException.class, () -> FeedSession.open(new Clients(UNAUTHORIZED)));

    assertSame(UNAUTHORIZED, thrown.getCause());
    assertFalse(thrown.isTransient());
  }

  @Test
  void failsTheResultOfAnOperationTheClientRefuses() {
    IllegalStateException closed = new IllegalStateException("Client is closed");
    FakeFeedClient refusing = new FakeFeedClient().answerAll(refusal(closed));
    FeedSession session = FeedSession.open(() -> refusing);

    List<CompletableFuture<Result>> results = session.submit(List.of(put(), put()));

    assertEquals(2, results.size());
    results.forEach(
        result ->
            assertSame(closed, assertThrows(CompletionException.class, result::join).getCause()));
  }

  @Test
  void sendsNothingUntilSubmitting() {
    FeedSession.open(() -> client);

    assertNothingSent(client);
  }

  private static Put put() {
    return new Put(RECORD, ID, "{\"fields\":{}}", OperationParameters.empty());
  }

  private static DocumentJson documentJson() {
    return new DocumentJson(RECORD, DOCUMENT_JSON, OperationParameters.empty());
  }

  private static void join(List<CompletableFuture<Result>> results) {
    results.forEach(CompletableFuture::join);
  }

  /**
   * Builds each client in turn; a {@link RuntimeException} stands for a client that fails to build.
   */
  private static final class Clients implements Supplier<FeedClient> {
    private final Deque<Object> outcomes;
    private int built;

    Clients(Object... outcomes) {
      this.outcomes = new ArrayDeque<>(List.of(outcomes));
    }

    @Override
    public FeedClient get() {
      built++;
      Object outcome = outcomes.size() > 1 ? outcomes.poll() : outcomes.peek();
      if (outcome instanceof RuntimeException) {
        throw (RuntimeException) outcome;
      }
      return (FeedClient) outcome;
    }
  }
}
