package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParseException;
import ai.vespa.feed.client.Result;
import com.fasterxml.jackson.core.JsonParseException;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Decides what an indexing result means for its record. Malformed documents and rejected results
 * ({@code conditionNotMet}) are reported to the errant record reporter, then skipped or failed according to
 * {@code vespa.behavior.on.malformed.documents}. Any other failure is treated as transient and fails the record.
 */
public class IndexingResultHandler {
    private static final Logger log = LoggerFactory.getLogger(IndexingResultHandler.class);

    private final VespaSinkConfig.BehaviorOnMalformedDoc behaviorOnMalformedDoc;
    private final ErrantRecordReporter reporter;

    /**
     * @param reporter the errant record reporter, or {@code null} when none is configured
     */
    public IndexingResultHandler(VespaSinkConfig config, ErrantRecordReporter reporter) {
        this.behaviorOnMalformedDoc = config.behaviorOnMalformedDoc;
        this.reporter = reporter;
    }

    /**
     * Returns a future that completes with the record's result once {@code future} completes and the record has
     * been reported where needed. It completes normally when the record succeeded or is skipped, and
     * exceptionally when the record must fail the batch.
     */
    public CompletableFuture<Result> handle(SinkRecord record, CompletableFuture<Result> future) {
        CompletableFuture<Result> promise = new CompletableFuture<>();

        future.whenComplete((result, throwable) -> {
            if (result == null) {
                if (isMalformed(throwable)) {
                    skipOrFail(record, throwable, ignoredResult(), promise);
                } else {
                    log.error(errorMessage(record), throwable);
                    promise.completeExceptionally(throwable);
                }
            } else if (result.type() == Result.Type.success) {
                promise.complete(result);
            } else {
                // Vespa rejected the operation, i.e. its test-and-set condition was not met.
                skipOrFail(record, new Throwable(result.toString()), result, promise);
            }
        });

        return promise;
    }

    private void skipOrFail(SinkRecord record, Throwable cause, Result skipped, CompletableFuture<Result> promise) {
        if (reporter != null) {
            reporter.report(record, cause);
        }

        switch (behaviorOnMalformedDoc) {
            case IGNORE:
                log.info(ignoreMessage(record), cause);
                promise.complete(skipped);
                break;
            case WARN:
                log.warn(ignoreMessage(record), cause);
                promise.complete(skipped);
                break;
            case FAIL:
            default:
                log.error(errorMessage(record), cause);
                promise.completeExceptionally(cause);
        }
    }

    private static String ignoreMessage(SinkRecord record) {
        return String.format("Failed to index document '%s'. Ignoring and will not index it.", record);
    }

    private static String errorMessage(SinkRecord record) {
        return String.format(
                "Failed to index document '%s'. To ignore future document like this, change the configuration '%s' to '%s'.",
                record,
                VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
                VespaSinkConfig.BehaviorOnMalformedDoc.IGNORE
        );
    }

    /**
     * The feed client exposes no typed status code, so HTTP 400 is recognised from the exception message. A
     * {@code ResultParseException} is never malformed: the response could not be parsed, so its status is unknown.
     */
    private static boolean isMalformed(Throwable throwable) {
        Throwable rootCause = Stream
                .iterate(throwable, Throwable::getCause)
                .filter(element -> element.getCause() == null)
                .findFirst()
                .orElse(throwable);

        String throwableString = throwable.toString().toLowerCase();

        return !(throwableString.contains("resultparseexception"))
                && (throwableString.contains("status 400")
                || throwableString.contains("string field value contains illegal code point")
                || rootCause instanceof OperationParseException
                || rootCause instanceof JsonParseException);
    }

    private static Result ignoredResult() {
        return new Result() {
            @Override
            public Type type() {
                return Type.success;
            }

            @Override
            public DocumentId documentId() {
                return null;
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
