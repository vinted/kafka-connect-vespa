package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedException;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertNothingSent;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertSent;
import static com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient.Answer.failure;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VespaFeedTest {
    private static final FeedException UNAUTHORIZED = new FeedException("Status 401 executing 'POST /document/v1/': unauthorized");
    private static final FeedException HANDSHAKE_FAILED = new FeedException(
            "failed handshake with server after PT30S", new java.net.ConnectException("Connection refused"));

    private final VespaSinkConfig config = new VespaSinkConfig(Map.of(
            VespaSinkConfig.NAMESPACE_CONFIG, "ns",
            VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt"));
    private final RecordingFeedClient client = new RecordingFeedClient();
    private long offset;

    @Test
    void rebuildsTheFeedClientOnceItsCircuitBreakerOpens() {
        RecordingFeedClient replacement = new RecordingFeedClient();
        Clients clients = new Clients(client, replacement);
        VespaFeed feed = VespaFeed.open(config, null, clients);

        // Once open, the client aborts every operation and never recovers.
        client.answerAll(failure(new FeedException("Operation aborted")));
        client.circuitBreakerState(FeedClient.CircuitBreaker.State.OPEN);

        feed.feed(batch());

        assertTrue(client.isClosed());
        assertEquals(2, clients.built);
        assertNothingSent(client);
        assertSent(replacement, "put id:ns:dt::key-" + (offset - 1) + " {\"fields\":{\"field\":\"value\"}}");
    }

    @Test
    void opensWhileVespaIsUnreachableAndConnectsWhenFeeding() {
        Clients clients = new Clients(HANDSHAKE_FAILED, HANDSHAKE_FAILED, client);

        VespaFeed feed = assertDoesNotThrow(() -> VespaFeed.open(config, null, clients));
        VespaFeedException thrown = assertThrows(VespaFeedException.class, () -> feed.feed(batch()));
        feed.feed(batch());

        assertTrue(thrown.isTransient());
        assertEquals(3, clients.built);
        assertSent(client, "put id:ns:dt::key-" + (offset - 1) + " {\"fields\":{\"field\":\"value\"}}");
    }

    @Test
    void failsToOpenOnOtherClientErrors() {
        UncheckedIOException missingCertificate = new UncheckedIOException(new NoSuchFileException("client.pem"));

        VespaFeedException thrown = assertThrows(VespaFeedException.class,
                () -> VespaFeed.open(config, null, new Clients(missingCertificate)));

        assertSame(missingCertificate, thrown.getCause());
        assertEquals("fatal failure: NoSuchFileException: client.pem", thrown.getMessage());
    }

    @Test
    void failsToOpenWhenTheHandshakeFailsForANonTransientReason() {
        VespaFeedException thrown = assertThrows(VespaFeedException.class,
                () -> VespaFeed.open(config, null, new Clients(UNAUTHORIZED)));

        assertSame(UNAUTHORIZED, thrown.getCause());
        assertFalse(thrown.isTransient());
    }

    @Test
    void failsWithEveryFailedOperationEvenWhenItIsNotAFeedException() {
        IllegalStateException closed = new IllegalStateException("Client is closed");
        client.answerAll(failure(closed));
        VespaFeed feed = VespaFeed.open(config, null, new Clients(client));

        VespaFeedException thrown = assertThrows(VespaFeedException.class,
                () -> feed.feed(List.of(record("key-1"), record("key-2"))));

        assertSame(closed, thrown.getCause());
        assertEquals("fatal failure in 2 operations: IllegalStateException: Client is closed", thrown.getMessage());
    }

    @Test
    void closesTheFeedClient() throws IOException {
        VespaFeed feed = VespaFeed.open(config, null, new Clients(client));

        feed.close();

        assertTrue(client.isClosed());
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
