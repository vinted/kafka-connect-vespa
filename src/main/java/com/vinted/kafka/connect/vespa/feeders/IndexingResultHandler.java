package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.FeedException;
import ai.vespa.feed.client.Result;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Failure;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Kind;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides what an indexing result means for its record. Malformed documents and rejected results
 * ({@code conditionNotMet}) are reported to the errant record reporter, then skipped or failed
 * according to {@code vespa.behavior.on.malformed.documents}; one the reporter fails to report is a
 * fatal failure. Any other failure fails the record.
 */
public class IndexingResultHandler {
  private static final Logger log = LoggerFactory.getLogger(IndexingResultHandler.class);

  private final VespaSinkConfig.BehaviorOnMalformedDoc behaviorOnMalformedDoc;
  private final RecordReporter reporter;

  IndexingResultHandler(
      VespaSinkConfig.BehaviorOnMalformedDoc behaviorOnMalformedDoc, RecordReporter reporter) {
    this.behaviorOnMalformedDoc = behaviorOnMalformedDoc;
    this.reporter = reporter;
  }

  /**
   * Returns a future that completes once {@code future} completes and the record has been reported
   * where needed, with why the record fails its batch, or empty when it succeeded or is skipped. It
   * never completes exceptionally.
   */
  CompletableFuture<Optional<Failure>> handle(SinkRecord record, CompletableFuture<Result> future) {
    return future.handle((result, throwable) -> outcome(record, result, throwable));
  }

  private Optional<Failure> outcome(SinkRecord record, Result result, Throwable throwable) {
    if (result != null && result.type() == Result.Type.success) {
      return Optional.empty();
    }

    if (result != null) {
      // Vespa rejected the operation, i.e. its test-and-set condition was not met.
      return skipOrFail(
          record,
          new Failure(
              Kind.REJECTED_RESULT, new FeedException(result.documentId(), result.toString())));
    }

    IndexingFailure failure = IndexingFailure.of(throwable);
    if (failure == IndexingFailure.MALFORMED_DOCUMENT) {
      return skipOrFail(record, new Failure(Kind.MALFORMED_DOCUMENT, throwable));
    }

    logFailure(record, failure, throwable);
    return Optional.of(new Failure(failure.kind(), throwable));
  }

  private static void logFailure(SinkRecord record, IndexingFailure failure, Throwable throwable) {
    if (failure == IndexingFailure.TRANSIENT) {
      // The task retries the batch and logs it once.
      log.debug("Failed to index {}.", RecordCoordinates.of(record), throwable);
      return;
    }

    log.error("Failed to index {}.", RecordCoordinates.of(record), throwable);
  }

  private Optional<Failure> skipOrFail(SinkRecord record, Failure failure) {
    Optional<Failure> unreported = reporter.report(record, failure.cause);
    if (unreported.isPresent()) {
      return unreported;
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
    return String.format(
        "Failed to index %s. Ignoring and will not index it.", RecordCoordinates.of(record));
  }

  private static String failMessage(SinkRecord record) {
    return String.format(
        "Failed to index %s. To ignore future document like this, change the configuration '%s' to "
            + "'%s'.",
        RecordCoordinates.of(record),
        VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
        VespaSinkConfig.BehaviorOnMalformedDoc.IGNORE);
  }
}
