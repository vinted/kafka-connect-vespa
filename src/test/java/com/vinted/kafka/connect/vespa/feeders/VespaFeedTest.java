package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedClient;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient.Answer.failure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VespaFeedTest {
    private static final ErrantRecordReporter NO_REPORTER = (record, error) -> CompletableFuture.completedFuture(null);

    private final VespaSinkConfig config = new VespaSinkConfig(Map.of(
            VespaSinkConfig.NAMESPACE_CONFIG, "ns",
            VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt"));
    private final RecordingFeedClient client = new RecordingFeedClient();
    private long offset;

    @Test
    void failsWithEveryFailedOperationEvenWhenItIsNotAFeedException() {
        IllegalStateException closed = new IllegalStateException("Client is closed");
        client.answerAll(failure(closed));
        VespaFeed feed = VespaFeed.open(config, NO_REPORTER, () -> client);

        VespaFeedException thrown = assertThrows(VespaFeedException.class,
                () -> feed.feed(List.of(record("key-1"), record("key-2"))));

        assertSame(closed, thrown.getCause());
        assertEquals("fatal failure in 2 operations: IllegalStateException: Client is closed", thrown.getMessage());
    }

    @Test
    void failsEveryOperationTheClientRefusesInsteadOfLosingTheOthers() {
        IllegalStateException closed = new IllegalStateException("Client is closed");
        FeedClient refusing = mock(FeedClient.class);
        when(refusing.put(any(), any(), any())).thenThrow(closed);
        VespaFeed feed = VespaFeed.open(config, NO_REPORTER, () -> refusing);

        VespaFeedException thrown = assertThrows(VespaFeedException.class,
                () -> feed.feed(List.of(record("key-1"), record("key-2"))));

        assertEquals("fatal failure in 2 operations: IllegalStateException: Client is closed", thrown.getMessage());
    }

    @Test
    void closesTheFeedClient() throws IOException {
        VespaFeed feed = VespaFeed.open(config, NO_REPORTER, () -> client);

        feed.close();

        assertTrue(client.isClosed());
    }

    private SinkRecord record(String key) {
        return new SinkRecord("topic", 0, Schema.STRING_SCHEMA, key, Schema.STRING_SCHEMA, "{\"field\":\"value\"}", offset++);
    }
}
