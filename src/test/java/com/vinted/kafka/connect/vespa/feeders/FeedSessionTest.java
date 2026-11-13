package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentJson;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Put;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Remove;
import com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertNothingSent;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertSent;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FeedSessionTest {
    private static final DocumentId ID = DocumentId.of("id:ns:dt::a");
    private static final String DOCUMENT_JSON = "{\"put\":\"id:ns:dt::a\",\"fields\":{}}";
    private static final SinkRecord RECORD = new SinkRecord("topic", 0, null, "a", Schema.STRING_SCHEMA, "{}", 7);
    private static final FeedException UNAUTHORIZED = new FeedException("Status 401 executing 'POST /document/v1/': unauthorized");
    private static final FeedException HANDSHAKE_FAILED = new FeedException(
            "failed handshake with server after PT30S", new java.net.ConnectException("Connection refused"));

    private final RecordingFeedClient client = new RecordingFeedClient();

    @Test
    void closesTheClientOnceWhenOnlyDocumentOperationsWereSent() throws IOException {
        FeedSession session = FeedSession.open(() -> client, OperationParameters.empty());
        join(session.submit(List.of(put(), new Remove(RECORD, ID, OperationParameters.empty()))));

        session.close();

        assertEquals(1, client.closes());
    }

    @Test
    void closesTheClientOnceAfterSendingDocumentJsonOperations() throws IOException {
        FeedSession session = FeedSession.open(() -> client, OperationParameters.empty());
        join(session.submit(List.of(documentJson(), documentJson())));

        session.close();

        assertEquals(1, client.closes());
    }

    @Test
    void sendsDocumentJsonOperationsWithTimeoutRouteAndTracelevelButNotConditionOrCreate() {
        VespaSinkConfig config = new VespaSinkConfig(Map.of(
                VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "20",
                VespaSinkConfig.OPERATION_ROUTE_CONFIG, "custom",
                VespaSinkConfig.OPERATION_TRACELEVEL_CONFIG, "3",
                VespaSinkConfig.OPERATION_CONDITION_CONFIG, "dt.locked == false",
                VespaSinkConfig.OPERATION_CREATE_IF_NON_EXISTENT_CONFIG, "true"));
        FeedSession session = FeedSession.open(() -> client, config.operationParameters());

        join(session.submit(List.of(documentJson())));

        OperationParameters expected = OperationParameters.empty()
                .timeout(Duration.ofMillis(20))
                .route("custom")
                .tracelevel(3);
        assertEquals(expected, client.operations().get(0).parameters);
    }

    @Test
    void replacesTheClientOnceItsCircuitBreakerOpens() {
        RecordingFeedClient replacement = new RecordingFeedClient();
        Clients clients = new Clients(client, replacement);
        FeedSession session = FeedSession.open(clients, OperationParameters.empty());

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

        FeedSession session = assertDoesNotThrow(() -> FeedSession.open(clients, OperationParameters.empty()));
        VespaFeedException thrown = assertThrows(VespaFeedException.class, () -> session.submit(List.of(put())));
        join(session.submit(List.of(put())));

        assertTrue(thrown.isTransient());
        assertEquals(3, clients.built);
        assertSent(client, "put id:ns:dt::a {\"fields\":{}}");
    }

    @Test
    void failsToOpenOnOtherClientErrors() {
        UncheckedIOException missingCertificate = new UncheckedIOException(new NoSuchFileException("client.pem"));

        VespaFeedException thrown = assertThrows(VespaFeedException.class,
                () -> FeedSession.open(new Clients(missingCertificate), OperationParameters.empty()));

        assertSame(missingCertificate, thrown.getCause());
        assertEquals("fatal failure: NoSuchFileException: client.pem", thrown.getMessage());
    }

    @Test
    void failsToOpenWhenTheHandshakeFailsForANonTransientReason() {
        VespaFeedException thrown = assertThrows(VespaFeedException.class,
                () -> FeedSession.open(new Clients(UNAUTHORIZED), OperationParameters.empty()));

        assertSame(UNAUTHORIZED, thrown.getCause());
        assertFalse(thrown.isTransient());
    }

    @Test
    void failsTheResultOfAnOperationTheClientRefuses() {
        IllegalStateException closed = new IllegalStateException("Client is closed");
        FeedClient refusing = mock(FeedClient.class);
        when(refusing.put(any(), any(), any())).thenThrow(closed);
        FeedSession session = FeedSession.open(() -> refusing, OperationParameters.empty());

        List<CompletableFuture<Result>> results = session.submit(List.of(put(), put()));

        assertEquals(2, results.size());
        results.forEach(result -> assertSame(closed, assertThrows(CompletionException.class, result::join).getCause()));
    }

    @Test
    void sendsNothingUntilSubmitting() {
        FeedSession.open(() -> client, OperationParameters.empty());

        assertNothingSent(client);
    }

    private static Put put() {
        return new Put(RECORD, ID, "{\"fields\":{}}", OperationParameters.empty());
    }

    private static DocumentJson documentJson() {
        return new DocumentJson(RECORD, DOCUMENT_JSON);
    }

    private static void join(List<CompletableFuture<Result>> results) {
        results.forEach(CompletableFuture::join);
    }

    /** Builds each client in turn; a {@link RuntimeException} stands for a client that fails to build. */
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
