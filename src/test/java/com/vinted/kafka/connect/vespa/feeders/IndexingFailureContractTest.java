package com.vinted.kafka.connect.vespa.feeders;

import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.FATAL;
import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.MALFORMED_DOCUMENT;
import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.TRANSIENT;
import static com.vinted.kafka.connect.vespa.mocks.MockVespaServer.acknowledge;
import static com.vinted.kafka.connect.vespa.mocks.MockVespaServer.neverAnswer;
import static com.vinted.kafka.connect.vespa.mocks.MockVespaServer.respond;
import static com.vinted.kafka.connect.vespa.mocks.MockVespaServer.vespaError;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedClientBuilder;
import ai.vespa.feed.client.Result;
import ai.vespa.feed.client.impl.GracePeriodCircuitBreaker;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.mocks.MockVespaServer;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.eclipse.jetty.server.Request;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Classifies what the real feed client produces against a local server, so a client upgrade cannot
 * silently change it.
 */
class IndexingFailureContractTest {
  private static final DocumentId ID = DocumentId.of("id:ns:dt::1");
  private static final String DOCUMENT = "{\"fields\":{\"title\":\"a\"}}";
  private static final String HTML = "text/html";
  private static final String BAD_GATEWAY_PAGE =
      "<html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad "
          + "Gateway</h1></center></body></html>";

  // The client retries 503s and network failures itself; fail them on the first attempt instead.
  private final Map<String, String> params =
      new HashMap<>(
          Map.of(
              VespaSinkConfig.RETRY_STRATEGY_RETRIES_CONFIG, "0",
              VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "2000",
              VespaSinkConfig.CIRCUIT_BREAKER_GRACE_PERIOD_MS_CONFIG, "100"));
  private MockVespaServer server;

  static Stream<Arguments> failedOperations() {
    return Stream.of(
        Arguments.of(
            "400 Vespa error",
            vespaError(400, "Error in document 'id:ns:dt::1' - could not parse field 'title'"),
            MALFORMED_DOCUMENT),
        Arguments.of("401 Vespa error", vespaError(401, "Unauthorized"), FATAL),
        Arguments.of("403 Vespa error", vespaError(403, "Forbidden"), FATAL),
        Arguments.of(
            "404 Vespa error", vespaError(404, "Nothing at '/document/v1/ns/dt/docid/1'"), FATAL),
        Arguments.of("500 Vespa error", vespaError(500, "Internal server error"), TRANSIENT),
        Arguments.of(
            "502 Vespa error", vespaError(502, "Could not send message to distributor"), TRANSIENT),
        Arguments.of(
            "503 Vespa error", vespaError(503, "Rejecting execution due to overload"), TRANSIENT),
        Arguments.of("504 Vespa error", vespaError(504, "Timed out"), TRANSIENT),
        Arguments.of("507 Vespa error", vespaError(507, "Disk full"), TRANSIENT),
        Arguments.of("502 HTML", respond(502, HTML, BAD_GATEWAY_PAGE), TRANSIENT),
        Arguments.of(
            "503 HTML",
            respond(503, HTML, "<html><body>Service Unavailable</body></html>"),
            TRANSIENT),
        // A body that is not Vespa JSON loses its status, so even a 4xx is retried.
        Arguments.of(
            "404 HTML", respond(404, HTML, "<html><body>Not Found</body></html>"), TRANSIENT),
        Arguments.of("401 empty", respond(401, null, ""), TRANSIENT));
  }

  static Stream<Arguments> rejectedHandshakes() {
    return Stream.of(
        Arguments.of("401 Vespa error", vespaError(401, "Unauthorized")),
        Arguments.of("403 Vespa error", vespaError(403, "Forbidden")),
        Arguments.of("503 Vespa error", vespaError(503, "Service unavailable")),
        Arguments.of("502 HTML", respond(502, HTML, BAD_GATEWAY_PAGE)),
        Arguments.of("503 plain text", respond(503, "text/plain", "no healthy upstream")),
        Arguments.of("503 empty", respond(503, null, "")));
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 404, 500, 503})
  void readsTheStatusOfVespaErrorsAndDescribesThemWithoutTheDocumentId(int status)
      throws Exception {
    server = MockVespaServer.http(vespaError(status, "Failed"));
    params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

    try (FeedClient client = config().feedClient()) {
      Throwable failure = failure(put(client, ID));

      assertEquals(Optional.of(status), IndexingFailure.status(failure));
      assertEquals(
          "FeedException: Status "
              + status
              + " executing 'POST /document/v1/ns/dt/docid/1': Failed",
          IndexingFailure.describe(failure));
    }
  }

  @ParameterizedTest(name = "{0} -> {2}")
  @MethodSource("failedOperations")
  void classifiesFailedOperations(String name, Request.Handler answer, IndexingFailure expected)
      throws Exception {
    server = MockVespaServer.http(answer);

    assertPutFails(expected);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("rejectedHandshakes")
  void classifiesRejectedHandshakeAsTransient(String name, Request.Handler handshake)
      throws Exception {
    server = MockVespaServer.http(handshake, neverAnswer());
    params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

    assertKind(TRANSIENT, assertThrows(RuntimeException.class, () -> config().feedClient()));
  }

  @Test
  void classifiesConnectionRefusedDuringHandshakeAsTransient() throws Exception {
    params.put(VespaSinkConfig.ENDPOINT_CONFIG, "http://localhost:" + unusedPort());

    assertKind(TRANSIENT, assertThrows(RuntimeException.class, () -> config().feedClient()));
  }

  @Test
  void classifiesConnectionRefusedAfterServerStopsAsTransient() throws Exception {
    server = MockVespaServer.http(acknowledge());
    params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

    try (FeedClient client = config().feedClient()) {
      server.close();

      assertKind(TRANSIENT, failure(put(client, ID)));
    }
  }

  @Test
  void classifiesOperationTimeoutAsTransient() throws Exception {
    params.put(VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "200");
    server = MockVespaServer.http(neverAnswer());

    assertPutFails(TRANSIENT);
  }

  @Test
  void classifiesThrottlingUntilTimeoutAsTransient() throws Exception {
    params.put(VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "200");
    server = MockVespaServer.http(vespaError(429, "Rejecting execution due to overload"));

    assertPutFails(TRANSIENT);
  }

  @Test
  void classifiesOperationsAbortedByOpenCircuitBreakerAsTransient() throws Exception {
    Request.Handler failing = vespaError(500, "Internal server error");
    Request.Handler hanging = neverAnswer();
    // The first operation starts the breaker's failure period; the second is in flight when it
    // opens.
    server =
        MockVespaServer.http(
            (request, response, callback) ->
                (request.getHttpURI().getPath().endsWith("/1") ? failing : hanging)
                    .handle(request, response, callback));
    params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

    // vespa.max.failure.ms is at least 10s, too slow for a test.
    try (FeedClient client =
        FeedClientBuilder.create(URI.create(server.url()))
            .setCircuitBreaker(
                new GracePeriodCircuitBreaker(Duration.ofMillis(100), Duration.ofMillis(300)))
            .build()) {
      failure(put(client, ID));

      Throwable aborted = failure(put(client, DocumentId.of("id:ns:dt::2")));

      assertEquals("Operation aborted", aborted.getMessage());
      assertKind(TRANSIENT, aborted);
    }
  }

  @Test
  void returnsConditionNotMetAsResultRatherThanFailure() throws Exception {
    server = MockVespaServer.http(vespaError(412, "Condition did not match document"));
    params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

    try (FeedClient client = config().feedClient()) {
      assertEquals(Result.Type.conditionNotMet, put(client, ID).get(5, SECONDS).type());
    }
  }

  @AfterEach
  void stopServer() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  private void assertPutFails(IndexingFailure expected) throws Exception {
    params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

    try (FeedClient client = config().feedClient()) {
      assertKind(expected, failure(put(client, ID)));
    }
  }

  private CompletableFuture<Result> put(FeedClient client, DocumentId id) {
    return client.put(id, DOCUMENT, config().operationParameters());
  }

  private VespaSinkConfig config() {
    return new VespaSinkConfig(params);
  }

  private static void assertKind(IndexingFailure expected, Throwable failure) {
    assertEquals(expected, IndexingFailure.of(failure), failure.toString());
  }

  private static Throwable failure(CompletableFuture<Result> operation) {
    return assertThrows(ExecutionException.class, () -> operation.get(5, SECONDS)).getCause();
  }

  private static int unusedPort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
