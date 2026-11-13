package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Failure;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Kind;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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
        this.behaviorOnMalformedDoc = config.behaviorOnMalformedDoc();
        this.reporter = reporter;
    }

    /**
     * Returns a future that completes once {@code future} completes and the record has been reported where needed,
     * with why the record fails its batch, or empty when it succeeded or is skipped. It never completes exceptionally.
     */
    CompletableFuture<Optional<Failure>> handle(SinkRecord record, CompletableFuture<Result> future) {
        return future
                .handle((result, throwable) -> outcome(record, result, throwable))
                .exceptionally(throwable -> unhandled(record, throwable));
    }

    private Optional<Failure> outcome(SinkRecord record, Result result, Throwable throwable) {
        if (result != null && result.type() == Result.Type.success) {
            return Optional.empty();
        }

        if (result != null) {
            // Vespa rejected the operation, i.e. its test-and-set condition was not met.
            return skipOrFail(record, new Failure(Kind.REJECTED_RESULT, new FeedException(result.documentId(), result.toString())));
        }

        IndexingFailure failure = IndexingFailure.of(throwable);
        if (failure == IndexingFailure.MALFORMED_DOCUMENT) {
            return skipOrFail(record, new Failure(Kind.MALFORMED_DOCUMENT, throwable));
        }

        logFailure(record, failure, throwable);
        return Optional.of(new Failure(Kind.of(failure), throwable));
    }

    // The errant record reporter throws "Tolerance exceeded" under errors.tolerance=none.
    private static Optional<Failure> unhandled(SinkRecord record, Throwable throwable) {
        Throwable cause = throwable instanceof CompletionException && throwable.getCause() != null ? throwable.getCause() : throwable;
        log.error("Failed to handle the indexing result of {}.", Records.describe(record), cause);
        return Optional.of(new Failure(Kind.FATAL, cause));
    }

    private static void logFailure(SinkRecord record, IndexingFailure failure, Throwable throwable) {
        if (failure == IndexingFailure.TRANSIENT) {
            // The task retries the batch and logs it once.
            log.debug("Failed to index {}.", Records.describe(record), throwable);
            return;
        }

        log.error("Failed to index {}.", Records.describe(record), throwable);
    }

    private Optional<Failure> skipOrFail(SinkRecord record, Failure failure) {
        if (reporter != null) {
            reporter.report(record, failure.cause);
        }

        switch (behaviorOnMalformedDoc) {
            case IGNORE:
                log.info(ignoreMessage(record), failure.cause);
                return Optional.empty();
            case WARN:
                log.warn(ignoreMessage(record), failure.cause);
                return Optional.empty();
            case FAIL:
            default:
                log.error(failMessage(record), failure.cause);
                return Optional.of(failure);
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
}
