package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.mocks.CallbackExecutor;
import com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient.Answer.failure;
import static com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient.Answer.result;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VespaSinkTaskRetryTest {
    private static final FeedException UNAVAILABLE = new FeedException("Status 503 executing 'POST /document/v1/': unavailable");
    private static final FeedException UNAUTHORIZED = new FeedException("Status 401 executing 'POST /document/v1/': unauthorized");
    private static final FeedException BAD_REQUEST = new FeedException("Status 400 executing 'POST /document/v1/': bad request");

    private final Map<String, String> params = new HashMap<>();
    private RecordingFeedClient client = new RecordingFeedClient();
    private final ConnectWorker worker = new ConnectWorker(new VespaSinkTask(config -> client));
    private long offset;

    @BeforeEach
    void before() {
        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "dt");
        params.put(VespaSinkConfig.MAX_RETRIES_CONFIG, "1");
        params.put(VespaSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
    }

    @AfterEach
    void after() {
        worker.stop();
    }

    @Test
    void asksConnectToBackOffBeforeRedeliveringATransientFailure() {
        client.answerAll(failure(UNAVAILABLE));
        worker.start(params);

        assertThrows(RetriableException.class, () -> worker.put(batch()));

        List<Long> backoffs = worker.backoffs();
        assertEquals(1, backoffs.size());
        assertTrue(backoffs.get(0) >= 0 && backoffs.get(0) < 2000, "backoff " + backoffs.get(0));
    }

    @Test
    void logsWhyVespaIsUnavailableWhenRetrying() {
        client.answerAll(failure(UNAVAILABLE));
        worker.start(params);

        try (CapturedLogs logs = CapturedLogs.capture()) {
            assertThrows(RetriableException.class, () -> worker.put(batch()));

            String warnings = String.join("\n", logs.warnings());
            assertTrue(warnings.contains("Vespa is unavailable") && warnings.contains("Status 503"), warnings);
        }
    }

    @Test
    void failsTheTaskOnceTheRetryBudgetIsExhausted() {
        client.answerAll(failure(UNAVAILABLE));
        worker.start(params);

        assertThrows(RetriableException.class, () -> worker.put(batch()));
        assertNotRetriable(() -> worker.put(batch()));

        assertEquals(1, worker.redeliveries());
    }

    @Test
    void startsTheRetryBudgetAgainAfterASuccessfulBatch() {
        worker.start(params);

        client.answerAll(failure(UNAVAILABLE));
        assertThrows(RetriableException.class, () -> worker.put(batch()));
        client.answerAll(result(Result.Type.success));
        worker.put(batch());
        client.answerAll(failure(UNAVAILABLE));

        assertThrows(RetriableException.class, () -> worker.put(batch()));
        assertEquals(2, worker.redeliveries());
    }

    @Test
    void failsTheTaskOnFailuresThatAreNotTransient() {
        client.answerAll(failure(UNAUTHORIZED));
        worker.start(params);

        assertNotRetriable(() -> worker.put(batch()));
        assertEquals(0, worker.redeliveries());
    }

    @Test
    void logsAFatalFailureOnceAndThatItFailsTheTask() {
        client.answerAll(failure(UNAUTHORIZED));
        SinkRecord unauthorized = record("unauthorized");
        worker.start(params);

        try (CapturedLogs logs = CapturedLogs.capture()) {
            assertNotRetriable(() -> worker.put(List.of(unauthorized)));

            List<String> errors = logs.errors();
            List<String> withStackTraces = errors.stream().filter(error -> error.contains("\tat ")).collect(Collectors.toList());
            assertEquals(1, withStackTraces.size(), String.join("\n", errors));
            assertTrue(withStackTraces.get(0).contains("offset=" + unauthorized.kafkaOffset()), withStackTraces.get(0));
            assertTrue(withStackTraces.get(0).contains("Status 401"), withStackTraces.get(0));
            assertTrue(errors.stream().anyMatch(error -> error.contains(VespaSinkTask.class.getName())), String.join("\n", errors));
        }
    }

    @Test
    void reportsASkippedMalformedDocumentOnceWhileItsBatchIsRedelivered() {
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
        SinkRecord malformed = record("malformed");
        List<SinkRecord> batch = List.of(malformed, record("unavailable"));
        client.answer("id:ns:dt::malformed", failure(BAD_REQUEST))
                .answer("id:ns:dt::unavailable", failure(UNAVAILABLE));
        worker.start(params);

        assertThrows(RetriableException.class, () -> worker.put(batch));
        assertNotRetriable(() -> worker.put(batch));

        assertEquals(List.of(malformed), worker.reported());
    }

    @Test
    void reportsEachSkippedRecordOnceWhileResultsArriveConcurrentlyOnCallbackThreads() throws Exception {
        params.put(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE");
        List<SinkRecord> batch = List.of(record("malformed-1"), record("malformed-2"), record("rejected"), record("unavailable"));

        try (CallbackExecutor callbacks = new CallbackExecutor(batch.size())) {
            client = new RecordingFeedClient(callbacks)
                    .answer("id:ns:dt::malformed-1", failure(BAD_REQUEST))
                    .answer("id:ns:dt::malformed-2", failure(BAD_REQUEST))
                    .answer("id:ns:dt::rejected", result(Result.Type.conditionNotMet))
                    .answer("id:ns:dt::unavailable", failure(UNAVAILABLE));
            worker.start(params);

            assertThrows(RetriableException.class, () -> worker.put(batch));
            assertNotRetriable(() -> worker.put(batch));
        }

        String callbackThread = " on " + CallbackExecutor.THREAD_NAME;
        assertEquals(List.of("malformed-1" + callbackThread, "malformed-2" + callbackThread, "rejected" + callbackThread),
                worker.reportedKeysAndThreads().stream().sorted().collect(Collectors.toList()));
        assertEquals(2 * batch.size(), client.operations().size(), "operations sent over both deliveries");
    }

    @Test
    void reportsADroppedInvalidMessageOnceWhileItsBatchIsRedelivered() {
        params.put(VespaSinkConfig.DROP_INVALID_MESSAGE_CONFIG, "true");
        SinkRecord invalid = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "{}", offset++);
        List<SinkRecord> batch = List.of(invalid, record("unavailable"));
        client.answerAll(failure(UNAVAILABLE));
        worker.start(params);

        assertThrows(RetriableException.class, () -> worker.put(batch));
        assertNotRetriable(() -> worker.put(batch));

        assertEquals(List.of(invalid), worker.reported());
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
