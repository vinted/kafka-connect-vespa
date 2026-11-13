package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedException;
import com.vinted.kafka.connect.vespa.mocks.MockVespaFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class VespaSinkTaskRetryTest {
    private static final FeedException UNAVAILABLE = new FeedException("Status 503 executing 'POST /document/v1/': unavailable");
    private static final FeedException UNAUTHORIZED = new FeedException("Status 401 executing 'POST /document/v1/': unauthorized");
    private static final FeedException HANDSHAKE_FAILED = new FeedException(
            "failed handshake with server after PT30S", new java.net.ConnectException("Connection refused"));

    private final Map<String, String> params = new HashMap<>();
    private final SinkTaskContext context = mock(SinkTaskContext.class);
    private final VespaSinkTask task = new VespaSinkTask();
    private final MockVespaFeedClient client = new MockVespaFeedClient();
    private long offset;

    @BeforeEach
    void before() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
        params.put(VespaSinkConfig.MAX_RETRIES_CONFIG, "2");
        params.put(VespaSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        task.initialize(context);
    }

    @AfterEach
    void after() {
        task.stop();
    }

    @Test
    void retriesTransientFailuresWithBackoff() {
        client.failure = UNAVAILABLE;
        task.start(params, client);

        assertThrows(RetriableException.class, () -> task.put(batch()));
        assertThrows(RetriableException.class, () -> task.put(batch()));

        ArgumentCaptor<Long> backoffs = ArgumentCaptor.forClass(Long.class);
        verify(context, times(2)).timeout(backoffs.capture());
        long first = backoffs.getAllValues().get(0);
        long second = backoffs.getAllValues().get(1);
        assertTrue(first >= 0 && first < 2000, "first backoff " + first);
        assertTrue(second >= 0 && second < 4000, "second backoff " + second);
    }

    @Test
    void failsOnceTheRetryBudgetIsExhausted() {
        client.failure = UNAVAILABLE;
        task.start(params, client);

        assertThrows(RetriableException.class, () -> task.put(batch()));
        assertThrows(RetriableException.class, () -> task.put(batch()));
        ConnectException thrown = assertThrows(ConnectException.class, () -> task.put(batch()));

        assertTrue(!(thrown instanceof RetriableException));
        verify(context, times(2)).timeout(anyLong());
    }

    @Test
    void resetsTheRetryBudgetAfterASuccessfulBatch() {
        params.put(VespaSinkConfig.MAX_RETRIES_CONFIG, "1");
        task.start(params, client);

        client.failure = UNAVAILABLE;
        assertThrows(RetriableException.class, () -> task.put(batch()));
        client.failure = null;
        task.put(batch());
        client.failure = UNAVAILABLE;

        assertThrows(RetriableException.class, () -> task.put(batch()));
    }

    @Test
    void doesNotRetryClientErrors() {
        client.failure = UNAUTHORIZED;
        task.start(params, client);

        assertNotRetriable(() -> task.put(batch()));
        verify(context, never()).timeout(anyLong());
    }

    @Test
    void doesNotRetryABatchWithAnyNonTransientFailure() {
        client.failure = UNAVAILABLE;
        client.failures.put(DocumentId.of("id:ns:dt::key-2"), UNAUTHORIZED);
        task.start(params, client);

        assertNotRetriable(() -> task.put(List.of(record("key-1"), record("key-2"))));
    }

    @Test
    void doesNotRetryInvalidMessages() {
        task.start(params, client);

        assertNotRetriable(() -> task.put(List.of(new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{}", 0))));
    }

    @Test
    void doesNotRetryWhenRetriesAreDisabled() {
        params.put(VespaSinkConfig.MAX_RETRIES_CONFIG, "0");
        client.failure = UNAVAILABLE;
        task.start(params, client);

        assertNotRetriable(() -> task.put(batch()));
    }

    @Test
    void recreatesTheFeedClientOnceItsCircuitBreakerOpens() {
        MockVespaFeedClient replacement = new MockVespaFeedClient();
        Clients clients = new Clients(client, replacement);
        task.start(params, clients);

        // Once open, the client aborts every operation and never recovers.
        client.failure = new FeedException("Operation aborted");
        client.circuitBreakerState = FeedClient.CircuitBreaker.State.OPEN;

        task.put(batch());

        assertTrue(client.closed);
        assertEquals(2, clients.built);
        replacement.assertAllDocumentIds("id:ns:dt::key-" + (offset - 1));
    }

    @Test
    void startsWhileVespaIsUnreachableAndConnectsWhenFeeding() {
        Clients clients = new Clients(HANDSHAKE_FAILED, HANDSHAKE_FAILED, client);

        assertDoesNotThrow(() -> task.start(params, clients));
        assertThrows(RetriableException.class, () -> task.put(batch()));
        task.put(batch());

        assertEquals(3, clients.built);
        client.assertAllDocumentIds("id:ns:dt::key-" + (offset - 1));
    }

    @Test
    void failsToStartOnOtherClientErrors() {
        UncheckedIOException missingCertificate = new UncheckedIOException(new NoSuchFileException("client.pem"));

        assertSame(missingCertificate, assertThrows(UncheckedIOException.class,
                () -> task.start(params, new Clients(missingCertificate))));
    }

    @Test
    void failsToStartWhenTheHandshakeFailsForANonTransientReason() {
        assertThrows(FeedException.class, () -> task.start(params, new Clients(UNAUTHORIZED)));
    }

    private static void assertNotRetriable(org.junit.jupiter.api.function.Executable put) {
        ConnectException thrown = assertThrows(ConnectException.class, put);
        assertTrue(!(thrown instanceof RetriableException), "expected a non-retriable failure");
    }

    private List<SinkRecord> batch() {
        return List.of(record("key-" + offset));
    }

    private SinkRecord record(String key) {
        return new SinkRecord("topic", 0, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, "{\"field\":\"value\"}", offset++);
    }

    /** Builds each client in turn; a {@link RuntimeException} stands for a client that fails to build. */
    private static final class Clients implements Function<VespaSinkConfig, FeedClient> {
        private final Deque<Object> outcomes;
        private int built;

        Clients(Object... outcomes) {
            this.outcomes = new ArrayDeque<>(List.of(outcomes));
        }

        @Override
        public FeedClient apply(VespaSinkConfig config) {
            built++;
            Object outcome = outcomes.size() > 1 ? outcomes.poll() : outcomes.peek();
            if (outcome instanceof RuntimeException) {
                throw (RuntimeException) outcome;
            }
            return (FeedClient) outcome;
        }
    }
}
