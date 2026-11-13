package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.mocks.CallbackExecutor;
import com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient.Answer.failure;
import static com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient.Answer.result;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VespaSinkTaskRetryTest {
    private static final FeedException UNAVAILABLE = new FeedException("Status 503 executing 'POST /document/v1/': unavailable");
    private static final FeedException UNAUTHORIZED = new FeedException("Status 401 executing 'POST /document/v1/': unauthorized");
    private static final FeedException BAD_REQUEST = new FeedException("Status 400 executing 'POST /document/v1/': bad request");

    private final Map<String, String> params = new HashMap<>();
    private final SinkTaskContext context = mock(SinkTaskContext.class);
    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    private RecordingFeedClient client = new RecordingFeedClient();
    private final VespaSinkTask task = new VespaSinkTask(config -> client);
    private long offset;

    @BeforeEach
    void before() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
        params.put(VespaSinkConfig.MAX_RETRIES_CONFIG, "1");
        params.put(VespaSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        when(context.errantRecordReporter()).thenReturn(reporter);
        task.initialize(context);
    }

    @AfterEach
    void after() {
        task.stop();
    }

    @Test
    void asksConnectToBackOffBeforeRedeliveringATransientFailure() {
        client.answerAll(failure(UNAVAILABLE));
        task.start(params);

        assertThrows(RetriableException.class, () -> task.put(batch()));

        ArgumentCaptor<Long> backoff = ArgumentCaptor.forClass(Long.class);
        verify(context).timeout(backoff.capture());
        assertTrue(backoff.getValue() >= 0 && backoff.getValue() < 2000, "backoff " + backoff.getValue());
    }

    @Test
    void failsTheTaskOnceTheRetryBudgetIsExhausted() {
        client.answerAll(failure(UNAVAILABLE));
        task.start(params);

        assertThrows(RetriableException.class, () -> task.put(batch()));
        assertNotRetriable(() -> task.put(batch()));

        verify(context, times(1)).timeout(anyLong());
    }

    @Test
    void failsTheTaskOnFailuresThatAreNotTransient() {
        client.answerAll(failure(UNAUTHORIZED));
        task.start(params);

        assertNotRetriable(() -> task.put(batch()));
        verify(context, never()).timeout(anyLong());
    }

    @Test
    void reportsASkippedMalformedDocumentOnceWhileItsBatchIsRedelivered() {
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
        SinkRecord malformed = record("malformed");
        List<SinkRecord> batch = List.of(malformed, record("unavailable"));
        client.answer("id:ns:dt::malformed", failure(BAD_REQUEST))
                .answer("id:ns:dt::unavailable", failure(UNAVAILABLE));
        task.start(params);

        assertThrows(RetriableException.class, () -> task.put(batch));
        assertNotRetriable(() -> task.put(batch));

        verify(reporter, times(1)).report(eq(malformed), any());
    }

    @Test
    void reportsEachSkippedRecordOnceWhileResultsArriveConcurrentlyOnCallbackThreads() throws Exception {
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
        Queue<String> reports = new ConcurrentLinkedQueue<>();
        when(context.errantRecordReporter()).thenReturn((record, error) -> {
            reports.add(record.key() + " on " + Thread.currentThread().getName());
            return CompletableFuture.completedFuture(null);
        });
        List<SinkRecord> batch = List.of(record("malformed-1"), record("malformed-2"), record("rejected"), record("unavailable"));

        try (CallbackExecutor callbacks = new CallbackExecutor(batch.size())) {
            client = new RecordingFeedClient(callbacks)
                    .answer("id:ns:dt::malformed-1", failure(BAD_REQUEST))
                    .answer("id:ns:dt::malformed-2", failure(BAD_REQUEST))
                    .answer("id:ns:dt::rejected", result(Result.Type.conditionNotMet))
                    .answer("id:ns:dt::unavailable", failure(UNAVAILABLE));
            task.start(params);

            assertThrows(RetriableException.class, () -> task.put(batch));
            assertNotRetriable(() -> task.put(batch));
        }

        String callbackThread = " on " + CallbackExecutor.THREAD_NAME;
        assertEquals(List.of("malformed-1" + callbackThread, "malformed-2" + callbackThread, "rejected" + callbackThread),
                reports.stream().sorted().collect(Collectors.toList()));
        assertEquals(2 * batch.size(), client.operations().size(), "operations sent over both deliveries");
    }

    @Test
    void reportsADroppedInvalidMessageOnceWhileItsBatchIsRedelivered() {
        params.put(VespaSinkConfig.DROP_INVALID_MESSAGE_CONFIG, "true");
        SinkRecord invalid = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{}", offset++);
        List<SinkRecord> batch = List.of(invalid, record("unavailable"));
        client.answerAll(failure(UNAVAILABLE));
        task.start(params);

        assertThrows(RetriableException.class, () -> task.put(batch));
        assertNotRetriable(() -> task.put(batch));

        verify(reporter, times(1)).report(eq(invalid), any());
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
