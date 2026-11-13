package com.vinted.kafka.connect.vespa;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private final Map<String, String> params = new HashMap<>();
    private final SinkTaskContext context = mock(SinkTaskContext.class);
    private final MockVespaFeedClient client = new MockVespaFeedClient();
    private final VespaSinkTask task = new VespaSinkTask(config -> client);
    private long offset;

    @BeforeEach
    void before() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
        params.put(VespaSinkConfig.MAX_RETRIES_CONFIG, "1");
        params.put(VespaSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        task.initialize(context);
    }

    @AfterEach
    void after() {
        task.stop();
    }

    @Test
    void asksConnectToBackOffBeforeRedeliveringATransientFailure() {
        client.failure = UNAVAILABLE;
        task.start(params);

        assertThrows(RetriableException.class, () -> task.put(batch()));

        ArgumentCaptor<Long> backoff = ArgumentCaptor.forClass(Long.class);
        verify(context).timeout(backoff.capture());
        assertTrue(backoff.getValue() >= 0 && backoff.getValue() < 2000, "backoff " + backoff.getValue());
    }

    @Test
    void failsTheTaskOnceTheRetryBudgetIsExhausted() {
        client.failure = UNAVAILABLE;
        task.start(params);

        assertThrows(RetriableException.class, () -> task.put(batch()));
        assertNotRetriable(() -> task.put(batch()));

        verify(context, times(1)).timeout(anyLong());
    }

    @Test
    void failsTheTaskOnFailuresThatAreNotTransient() {
        client.failure = UNAUTHORIZED;
        task.start(params);

        assertNotRetriable(() -> task.put(batch()));
        verify(context, never()).timeout(anyLong());
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
}
