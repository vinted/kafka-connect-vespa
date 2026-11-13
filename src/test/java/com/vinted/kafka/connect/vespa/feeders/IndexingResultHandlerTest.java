package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.OperationParseException;
import ai.vespa.feed.client.Result;
import ai.vespa.feed.client.ResultException;
import ai.vespa.feed.client.ResultParseException;
import com.fasterxml.jackson.core.JsonParseException;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class IndexingResultHandlerTest {
    private static final DocumentId ID = DocumentId.of("id:ns:dt::1");

    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    private final SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "1", Schema.STRING_SCHEMA, "{}", 7);

    static Stream<Arguments> failures() {
        return Stream.of(
                Arguments.of("HTTP 400", new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request"), true),
                Arguments.of("illegal code point", new FeedException(ID, "String field value contains illegal code point 0x1"), true),
                Arguments.of("unparseable operation", new OperationParseException("No document id for document at offset 0"), true),
                Arguments.of("JSON parse error in cause chain", new OperationParseException("Failed to parse document JSON", new JsonParseException(null, "Unexpected character")), true),
                Arguments.of("unparseable result, even with status 400", new ResultParseException(ID, "Status 400: failed parsing response"), false),
                Arguments.of("Vespa failure (502/504/507)", new ResultException(ID, "Status 507: insufficient storage", null), false),
                Arguments.of("timeout", new FeedException(ID, new TimeoutException("operation timed out")), false),
                Arguments.of("malformed document id in RAW mode", new FeedException(new IllegalArgumentException("Document ID must be on the form 'id:...'")), false)
        );
    }

    @ParameterizedTest(name = "{0} -> malformed: {2}")
    @MethodSource("failures")
    void classifiesFailures(String name, Throwable failure, boolean malformed) {
        CompletableFuture<Result> promise = handle("IGNORE", CompletableFuture.failedFuture(failure));

        if (malformed) {
            assertEquals(Result.Type.success, promise.join().type());
            verify(reporter).report(record, failure);
        } else {
            CompletionException thrown = assertThrows(CompletionException.class, promise::join);
            assertSame(failure, thrown.getCause());
            verify(reporter, never()).report(any(), any());
        }
    }

    @Test
    void skipsMalformedDocumentsUnderWarn() {
        Throwable failure = new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request");

        CompletableFuture<Result> promise = handle("WARN", CompletableFuture.failedFuture(failure));

        assertEquals(Result.Type.success, promise.join().type());
        verify(reporter).report(record, failure);
    }

    @Test
    void reportsAndFailsMalformedDocumentsUnderFail() {
        Throwable failure = new FeedException(ID, "Status 400 executing 'POST /document/v1/': bad request");

        CompletableFuture<Result> promise = handle("FAIL", CompletableFuture.failedFuture(failure));

        CompletionException thrown = assertThrows(CompletionException.class, promise::join);
        assertSame(failure, thrown.getCause());
        verify(reporter).report(record, failure);
    }

    @Test
    void reportsAndSkipsRejectedResultsUnderIgnore() {
        Result rejected = result(Result.Type.conditionNotMet);

        CompletableFuture<Result> promise = handle("IGNORE", CompletableFuture.completedFuture(rejected));

        assertSame(rejected, promise.join());
        verify(reporter).report(any(), any());
    }

    @Test
    void reportsAndFailsRejectedResultsUnderFail() {
        CompletableFuture<Result> promise = handle("FAIL", CompletableFuture.completedFuture(result(Result.Type.conditionNotMet)));

        assertThrows(CompletionException.class, promise::join);
        verify(reporter).report(any(), any());
    }

    @Test
    void failsRejectedResultsWithFeedExceptionSoTheCauseSurvivesAwait() {
        CompletableFuture<Result> promise = handle("FAIL", CompletableFuture.completedFuture(result(Result.Type.conditionNotMet)));

        CompletionException thrown = assertThrows(CompletionException.class, promise::join);
        FeedException cause = assertInstanceOf(FeedException.class, thrown.getCause());
        assertEquals(Optional.of(ID), cause.documentId());
    }

    @Test
    void failsRecordInsteadOfHangingWhenErrantRecordReporterThrows() {
        ConnectException toleranceExceeded = new ConnectException("Tolerance exceeded in error handler");
        when(reporter.report(any(), any())).thenThrow(toleranceExceeded);
        Throwable failure = new FeedException(ID, "Status 400 executing POST: bad request");

        CompletableFuture<Result> promise = handle("IGNORE", CompletableFuture.failedFuture(failure));

        assertTrue(promise.isDone(), "promise must complete when the reporter throws");
        CompletionException thrown = assertThrows(CompletionException.class, promise::join);
        FeedException cause = assertInstanceOf(FeedException.class, thrown.getCause());
        assertSame(toleranceExceeded, cause.getCause());
    }

    @Test
    void passesSuccessfulResultsThrough() {
        Result success = result(Result.Type.success);

        CompletableFuture<Result> promise = handle("FAIL", CompletableFuture.completedFuture(success));

        assertSame(success, promise.join());
        verify(reporter, never()).report(any(), any());
    }

    private CompletableFuture<Result> handle(String behaviorOnMalformedDocs, CompletableFuture<Result> future) {
        VespaSinkConfig config = new VespaSinkConfig(
                Map.of(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, behaviorOnMalformedDocs));
        return new IndexingResultHandler(config, reporter).handle(record, future);
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
