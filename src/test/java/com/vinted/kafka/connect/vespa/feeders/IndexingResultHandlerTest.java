package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.OperationParseException;
import ai.vespa.feed.client.Result;
import com.fasterxml.jackson.core.JsonParseException;
import com.vinted.kafka.connect.vespa.CapturedLogs;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Failure;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Kind;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class IndexingResultHandlerTest {
    private static final DocumentId ID = DocumentId.of("id:ns:dt::1");

    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    private final SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "1", Schema.STRING_SCHEMA, "{}", 7);

    static Stream<Arguments> failuresThatAreNotMalformed() {
        return Stream.of(
                Arguments.of("transient failure", new FeedException(ID, new TimeoutException("operation timed out")), Kind.TRANSIENT),
                Arguments.of("fatal failure", new FeedException(new IllegalArgumentException("Document ID must be on the form 'id:...'")), Kind.FATAL)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("failuresThatAreNotMalformed")
    void failsFailuresThatAreNotMalformedWithoutReportingThem(String name, Throwable failure, Kind kind) {
        Failure failed = failure(handle("IGNORE", CompletableFuture.failedFuture(failure)));

        assertEquals(kind, failed.kind);
        assertSame(failure, failed.cause);
        verify(reporter, never()).report(any(), any());
    }

    @Test
    void logsFatalFailuresWithoutAdviceThatCannotHelpThem() {
        Throwable failure = new FeedException(ID, "Status 401 executing 'POST /document/v1/': unauthorized");

        try (CapturedLogs logs = CapturedLogs.capture()) {
            handle("FAIL", CompletableFuture.failedFuture(failure));

            String error = single(logs.errors());
            assertTrue(error.contains("topic=topic partition=0 offset=7"), error);
            assertTrue(error.contains("Status 401"), error);
            assertFalse(error.contains(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG), error);
        }
    }

    @Test
    void advisesIgnoringMalformedDocumentsThatFail() {
        Throwable failure = new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request");

        try (CapturedLogs logs = CapturedLogs.capture()) {
            handle("FAIL", CompletableFuture.failedFuture(failure));

            String error = single(logs.errors());
            assertTrue(error.contains("topic=topic partition=0 offset=7"), error);
            assertTrue(error.contains(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG), error);
        }
    }

    @Test
    void skipsMalformedDocumentsUnderIgnore() {
        Throwable failure = new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request");

        assertEquals(Optional.empty(), handle("IGNORE", CompletableFuture.failedFuture(failure)).join());
        verify(reporter).report(record, failure);
    }

    @Test
    void skipsMalformedDocumentsUnderWarn() {
        Throwable failure = new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request");

        assertEquals(Optional.empty(), handle("WARN", CompletableFuture.failedFuture(failure)).join());
        verify(reporter).report(record, failure);
    }

    @Test
    void reportsAndFailsMalformedDocumentsUnderFail() {
        Throwable failure = new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request");

        Failure failed = failure(handle("FAIL", CompletableFuture.failedFuture(failure)));

        assertEquals(Kind.MALFORMED_DOCUMENT, failed.kind);
        assertSame(failure, failed.cause);
        verify(reporter).report(record, failure);
    }

    @Test
    void reportsAndSkipsRejectedResultsUnderIgnore() {
        assertEquals(Optional.empty(), handle("IGNORE", CompletableFuture.completedFuture(result(Result.Type.conditionNotMet))).join());
        verify(reporter).report(any(), any());
    }

    @Test
    void reportsAndFailsRejectedResultsUnderFail() {
        Failure failed = failure(handle("FAIL", CompletableFuture.completedFuture(result(Result.Type.conditionNotMet))));

        assertEquals(Kind.REJECTED_RESULT, failed.kind);
        verify(reporter).report(any(), any());
    }

    @Test
    void failsRejectedResultsWithAFeedExceptionForTheirDocument() {
        Failure failed = failure(handle("FAIL", CompletableFuture.completedFuture(result(Result.Type.conditionNotMet))));

        FeedException cause = assertInstanceOf(FeedException.class, failed.cause);
        assertEquals(Optional.of(ID), cause.documentId());
    }

    @Test
    void failsRecordInsteadOfHangingWhenErrantRecordReporterThrows() {
        Throwable failure = new FeedException(ID, "Status 400 executing POST: bad request");
        ConnectException toleranceExceeded = toleranceExceeded(failure);
        when(reporter.report(any(), any())).thenThrow(toleranceExceeded);

        CompletableFuture<Optional<Failure>> outcome = handle("IGNORE", CompletableFuture.failedFuture(failure));

        assertTrue(outcome.isDone(), "outcome must complete when the reporter throws");
        Failure failed = failure(outcome);
        assertEquals(Kind.FATAL, failed.kind);
        assertSame(toleranceExceeded, failed.cause);
    }

    @Test
    void failsWithAFatalFailureWhenErrantRecordReporterThrowsOverAnUnparseableDocument() {
        Throwable unparseable = new OperationParseException("Failed to parse document JSON", new JsonParseException(null, "Unexpected character"));
        when(reporter.report(any(), any())).thenThrow(toleranceExceeded(unparseable));

        assertEquals(Kind.FATAL, failure(handle("IGNORE", CompletableFuture.failedFuture(unparseable))).kind);
    }

    @Test
    void logsAFailureToHandleTheIndexingResultWithItsCause() {
        Throwable failure = new FeedException(ID, "Status 400 executing POST: bad request");
        when(reporter.report(any(), any())).thenThrow(toleranceExceeded(failure));

        try (CapturedLogs logs = CapturedLogs.capture()) {
            handle("IGNORE", CompletableFuture.failedFuture(failure));

            String error = single(logs.errors());
            assertTrue(error.contains("topic=topic partition=0 offset=7"), error);
            assertTrue(error.contains("Tolerance exceeded in error handler"), error);
            assertFalse(error.contains(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG), error);
        }
    }

    @Test
    void hasNoFailureForSuccessfulResults() {
        assertEquals(Optional.empty(), handle("FAIL", CompletableFuture.completedFuture(result(Result.Type.success))).join());
        verify(reporter, never()).report(any(), any());
    }

    private static Failure failure(CompletableFuture<Optional<Failure>> outcome) {
        return outcome.join().orElseThrow();
    }

    private CompletableFuture<Optional<Failure>> handle(String behaviorOnMalformedDocs, CompletableFuture<Result> future) {
        VespaSinkConfig config = new VespaSinkConfig(
                Map.of(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, behaviorOnMalformedDocs));
        return new IndexingResultHandler(config, reporter).handle(record, future);
    }

    // As Kafka's RetryWithToleranceOperator throws it under errors.tolerance=none.
    private static ConnectException toleranceExceeded(Throwable reported) {
        return new ConnectException("Tolerance exceeded in error handler", reported);
    }

    private static String single(List<String> errors) {
        assertEquals(1, errors.size(), String.join("\n", errors));
        return errors.get(0);
    }

    private static Result result(Result.Type type) {
        return new Result() {
            @Override
            public Type type() {
                return type;
            }

            @Override
            public DocumentId documentId() {
                return ID;
            }

            @Override
            public Optional<String> resultMessage() {
                return Optional.empty();
            }

            @Override
            public Optional<String> traceMessage() {
                return Optional.empty();
            }
        };
    }
}
