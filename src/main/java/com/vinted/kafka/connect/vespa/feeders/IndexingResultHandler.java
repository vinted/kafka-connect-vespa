package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Decides what an indexing result means for its record. Malformed documents and rejected results
 * ({@code conditionNotMet}) are reported to the errant record reporter, then skipped or failed according to
 * {@code vespa.behavior.on.malformed.documents}. Any other failure fails the record.
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
            try {
                complete(record, result, throwable, promise);
            } catch (Throwable t) {
                // whenComplete swallows anything thrown here, which would leave the promise, and the feed awaiting it,
                // waiting forever. The errant record reporter throws "Tolerance exceeded" under errors.tolerance=none.
                promise.completeExceptionally(new FeedException(
                        "Failed to handle the indexing result of " + Records.describe(record), t));
            }
        });

        return promise;
    }

    private void complete(SinkRecord record, Result result, Throwable throwable, CompletableFuture<Result> promise) {
        if (result != null && result.type() == Result.Type.success) {
            promise.complete(result);
            return;
        }

        if (result != null) {
            // Vespa rejected the operation, i.e. its test-and-set condition was not met.
            skipOrFail(record, new FeedException(result.documentId(), result.toString()), result, promise);
            return;
        }

        IndexingFailure failure = IndexingFailure.of(throwable);
        if (failure == IndexingFailure.MALFORMED_DOCUMENT) {
            skipOrFail(record, throwable, ignoredResult(), promise);
            return;
        }

        logFailure(record, failure, throwable);
        promise.completeExceptionally(throwable);
    }

    private static void logFailure(SinkRecord record, IndexingFailure failure, Throwable throwable) {
        if (failure == IndexingFailure.TRANSIENT) {
            // The task retries the batch and logs it once.
            log.debug("Failed to index {}.", Records.describe(record), throwable);
            return;
        }

        log.error("Failed to index {}.", Records.describe(record), throwable);
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
                log.error(failMessage(record), cause);
                promise.completeExceptionally(cause);
        }
    }

    private static String ignoreMessage(SinkRecord record) {
        return String.format("Failed to index %s. Ignoring and will not index it.", Records.describe(record));
    }

    private static String failMessage(SinkRecord record) {
        return String.format(
                "Failed to index %s. To ignore future document like this, change the configuration '%s' to '%s'.",
                Records.describe(record),
                VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
                VespaSinkConfig.BehaviorOnMalformedDoc.IGNORE
        );
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
