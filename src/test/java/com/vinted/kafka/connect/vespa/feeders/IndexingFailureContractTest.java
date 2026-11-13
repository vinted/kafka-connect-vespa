package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.FeedClientBuilder;
import ai.vespa.feed.client.Result;
import ai.vespa.feed.client.impl.GracePeriodCircuitBreaker;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.factories.VespaFeedClientFactory;
import com.vinted.kafka.connect.vespa.mocks.MockVespaServer;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.FATAL;
import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.MALFORMED_DOCUMENT;
import static com.vinted.kafka.connect.vespa.feeders.IndexingFailure.TRANSIENT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Classifies what the real feed client produces against a local server, so a client upgrade cannot silently change it. */
class IndexingFailureContractTest {
    private static final DocumentId ID = DocumentId.of("id:ns:dt::1");
    private static final String DOCUMENT = "{\"fields\":{\"title\":\"a\"}}";
    private static final String HANDSHAKE_PATH = "/document/v1/feeder/handshake/docid/dummy";
    private static final String JSON = "application/json";
    private static final String HTML = "text/html";
    private static final String BAD_GATEWAY_PAGE =
            "<html><head><title>502 Bad Gateway</title></head><body><center><h1>502 Bad Gateway</h1></center></body></html>";
    private static final Request.Handler HANDSHAKE_OK = respond(200, JSON, "{}");
    private static final Request.Handler NEVER_RESPOND = (request, response, callback) -> true;

    // The client retries 503s and network failures itself; fail them on the first attempt instead.
    private final Map<String, String> params = new HashMap<>(Map.of(
            VespaSinkConfig.RETRY_STRATEGY_RETRIES_CONFIG, "0",
            VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "2000",
            VespaSinkConfig.CIRCUIT_BREAKER_GRACE_PERIOD_MS_CONFIG, "100"));
    private MockVespaServer server;

    static Stream<Arguments> failedOperations() {
        return Stream.of(
                Arguments.of(400, JSON, vespaError("Error in document 'id:ns:dt::1' - could not parse field 'title'"), MALFORMED_DOCUMENT),
                Arguments.of(401, JSON, vespaError("Unauthorized"), FATAL),
                Arguments.of(403, JSON, vespaError("Forbidden"), FATAL),
                Arguments.of(404, JSON, vespaError("Nothing at '/document/v1/ns/dt/docid/1'"), FATAL),
                Arguments.of(500, JSON, vespaError("Internal server error"), TRANSIENT),
                Arguments.of(502, JSON, vespaError("Could not send message to distributor"), TRANSIENT),
                Arguments.of(503, JSON, vespaError("Rejecting execution due to overload"), TRANSIENT),
                Arguments.of(504, JSON, vespaError("Timed out"), TRANSIENT),
                Arguments.of(507, JSON, vespaError("Disk full"), TRANSIENT),
                Arguments.of(502, HTML, BAD_GATEWAY_PAGE, TRANSIENT),
                Arguments.of(503, HTML, "<html><body>Service Unavailable</body></html>", TRANSIENT),
                // A body that is not Vespa JSON loses its status, so even a 4xx is retried.
                Arguments.of(404, HTML, "<html><body>Not Found</body></html>", TRANSIENT),
                Arguments.of(401, null, "", TRANSIENT));
    }

    static Stream<Arguments> rejectedHandshakes() {
        return Stream.of(
                Arguments.of(401, JSON, vespaError("Unauthorized")),
                Arguments.of(403, JSON, vespaError("Forbidden")),
                Arguments.of(503, JSON, vespaError("Service unavailable")),
                Arguments.of(502, HTML, BAD_GATEWAY_PAGE),
                Arguments.of(503, "text/plain", "no healthy upstream"));
    }

    @ParameterizedTest(name = "{0} {1} -> {3}")
    @MethodSource("failedOperations")
    void classifiesFailedOperations(int status, String contentType, String body, IndexingFailure expected) throws Exception {
        server = MockVespaServer.http(afterHandshake(respond(status, contentType, body)));

        assertPutFails(expected);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("rejectedHandshakes")
    void classifiesRejectedHandshakeAsTransient(int status, String contentType, String body) throws Exception {
        server = MockVespaServer.http(handler(respond(status, contentType, body)));
        params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

        assertKind(TRANSIENT, assertThrows(RuntimeException.class, () -> VespaFeedClientFactory.create(config())));
    }

    @Test
    void classifiesConnectionRefusedDuringHandshakeAsTransient() throws Exception {
        params.put(VespaSinkConfig.ENDPOINT_CONFIG, "http://localhost:" + unusedPort());

        assertKind(TRANSIENT, assertThrows(RuntimeException.class, () -> VespaFeedClientFactory.create(config())));
    }

    @Test
    void classifiesConnectionRefusedAfterServerStopsAsTransient() throws Exception {
        server = MockVespaServer.http(afterHandshake(HANDSHAKE_OK));
        params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

        try (FeedClient client = VespaFeedClientFactory.create(config())) {
            server.close();

            assertKind(TRANSIENT, failure(put(client, ID)));
        }
    }

    @Test
    void classifiesOperationTimeoutAsTransient() throws Exception {
        params.put(VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "200");
        server = MockVespaServer.http(afterHandshake(NEVER_RESPOND));

        assertPutFails(TRANSIENT);
    }

    @Test
    void classifiesThrottlingUntilTimeoutAsTransient() throws Exception {
        params.put(VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "200");
        server = MockVespaServer.http(afterHandshake(respond(429, JSON, vespaError("Rejecting execution due to overload"))));

        assertPutFails(TRANSIENT);
    }

    @Test
    void classifiesOperationsAbortedByOpenCircuitBreakerAsTransient() throws Exception {
        Request.Handler failing = respond(500, JSON, vespaError("Internal server error"));
        // The first operation starts the breaker's failure period; the second is in flight when it opens.
        server = MockVespaServer.http(afterHandshake((request, response, callback) ->
                (request.getHttpURI().getPath().endsWith("/1") ? failing : NEVER_RESPOND).handle(request, response, callback)));
        params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

        // vespa.max.failure.ms is at least 10s, too slow for a test.
        try (FeedClient client = FeedClientBuilder.create(URI.create(server.url()))
                .setCircuitBreaker(new GracePeriodCircuitBreaker(Duration.ofMillis(100), Duration.ofMillis(300)))
                .build()) {
            failure(put(client, ID));

            Throwable aborted = failure(put(client, DocumentId.of("id:ns:dt::2")));

            assertEquals("Operation aborted", aborted.getMessage());
            assertKind(TRANSIENT, aborted);
        }
    }

    @Test
    void returnsConditionNotMetAsResultRatherThanFailure() throws Exception {
        server = MockVespaServer.http(afterHandshake(respond(412, JSON, vespaError("Condition did not match document"))));
        params.put(VespaSinkConfig.ENDPOINT_CONFIG, server.url());

        try (FeedClient client = VespaFeedClientFactory.create(config())) {
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

        try (FeedClient client = VespaFeedClientFactory.create(config())) {
            assertKind(expected, failure(put(client, ID)));
        }
    }

    private CompletableFuture<Result> put(FeedClient client, DocumentId id) {
        return client.put(id, DOCUMENT, OperationParameters.empty().timeout(config().timeout));
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

    private static String vespaError(String message) {
        return "{\"pathId\":\"/document/v1/ns/dt/docid/1\",\"id\":\"id:ns:dt::1\",\"message\":\"" + message + "\"}";
    }

    private static Request.Handler respond(int status, String contentType, String body) {
        return (request, response, callback) -> {
            response.setStatus(status);
            if (contentType != null) {
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, contentType);
            }
            Content.Sink.write(response, true, body, callback);
            return true;
        };
    }

    private static Handler afterHandshake(Request.Handler operations) {
        return handler((request, response, callback) -> {
            boolean handshake = request.getHttpURI().getPath().equals(HANDSHAKE_PATH);
            return (handshake ? HANDSHAKE_OK : operations).handle(request, response, callback);
        });
    }

    private static Handler handler(Request.Handler handler) {
        return new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) throws Exception {
                return handler.handle(request, response, callback);
            }
        };
    }
}
