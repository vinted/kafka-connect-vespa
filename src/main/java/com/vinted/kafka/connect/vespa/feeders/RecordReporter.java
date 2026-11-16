package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Failure;
import com.vinted.kafka.connect.vespa.feeders.VespaFeedException.Kind;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Reports failed records to the errant record reporter, each once until {@link #forget()}. A
 * reporter that throws, as it does once {@code errors.tolerance=none} is exceeded, is a fatal
 * failure of the record.
 */
final class RecordReporter {
  private final ErrantRecordReporter reporter;
  // Written from feed client callback threads as well as the task thread.
  private final Set<RecordCoordinates> reported = ConcurrentHashMap.newKeySet();

  RecordReporter(ErrantRecordReporter reporter) {
    this.reporter = reporter;
  }

  /**
   * Reports the record with why it failed, unless it was reported already; returns a fatal failure
   * when the reporter throws.
   */
  Optional<Failure> report(SinkRecord record, Throwable error) {
    RecordCoordinates coordinates = RecordCoordinates.of(record);
    if (!reported.add(coordinates)) {
      return Optional.empty();
    }

    try {
      reporter.report(record, error);
      return Optional.empty();
    } catch (ConnectException e) {
      return Optional.of(new Failure(Kind.FATAL, unreported(coordinates, error, e)));
    }
  }

  /** Forgets which records were reported, once their batch is delivered or given up on. */
  void forget() {
    reported.clear();
  }

  // The record's failure, also the cause of the reporter's, can echo record content, so only its
  // type
  // is kept.
  private static ConnectException unreported(
      RecordCoordinates coordinates, Throwable error, ConnectException e) {
    return new ConnectException(
        String.format(
            "Failed to report %s, which failed with %s, to the errant record reporter: %s",
            coordinates, error.getClass().getSimpleName(), e.getMessage()));
  }
}
