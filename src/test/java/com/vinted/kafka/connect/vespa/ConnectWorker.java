package com.vinted.kafka.connect.vespa;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Plays the Connect worker for one sink task: starts it, hands it batches, redelivers a batch after the backoff the
 * task asks for, and keeps what the task reported to the errant record reporter and the backoffs it asked for.
 */
final class ConnectWorker {
    private final VespaSinkTask task;
    private final Queue<Report> reports = new ConcurrentLinkedQueue<>();
    private final Queue<Long> backoffs = new ConcurrentLinkedQueue<>();
    private volatile ReporterAccess reporterAccess = ReporterAccess.RECORDING;
    private volatile RuntimeException reportFailure;
    private volatile Error runtimeWithoutReporter;
    private volatile long pendingBackoffMs;
    private boolean started;

    /** What one call to the errant record reporter received, and on which thread. */
    static final class Report {
        final SinkRecord record;
        final Throwable error;
        final String thread;

        private Report(SinkRecord record, Throwable error, String thread) {
            this.record = record;
            this.error = error;
            this.thread = thread;
        }
    }

    private enum ReporterAccess { RECORDING, ABSENT, UNSUPPORTED }

    ConnectWorker(VespaSinkTask task) {
        this.task = task;
        task.initialize(new Context());
    }

    ConnectWorker start(Map<String, String> props) {
        task.start(props);
        started = true;
        return this;
    }

    void stop() {
        if (!started) {
            return;
        }
        started = false;
        task.stop();
    }

    void restart(Map<String, String> props) {
        stop();
        start(props);
    }

    /** Hands the task one batch, as Connect does, letting whatever it throws through. */
    void put(List<SinkRecord> batch) {
        task.put(batch);
    }

    /** Hands the task one batch until it accepts it, waiting the backoff it asks for before each redelivery. */
    void deliver(List<SinkRecord> batch) throws InterruptedException {
        while (true) {
            try {
                task.put(batch);
                return;
            } catch (RetriableException e) {
                Thread.sleep(takeBackoffMs());
            }
        }
    }

    /** Delivers the records in batches of the given size, returning how long each batch took in nanoseconds. */
    List<Long> deliver(List<SinkRecord> records, int batchSize) throws InterruptedException {
        List<Long> nanos = new ArrayList<>();
        for (int from = 0; from < records.size(); from += batchSize) {
            long start = System.nanoTime();
            deliver(records.subList(from, Math.min(records.size(), from + batchSize)));
            nanos.add(System.nanoTime() - start);
        }
        return nanos;
    }

    List<Report> reports() {
        return List.copyOf(reports);
    }

    List<SinkRecord> reported() {
        return reports.stream().map(report -> report.record).collect(Collectors.toList());
    }

    /** The key of each reported record, with the thread it was reported on, as "key on thread". */
    List<String> reportedKeysAndThreads() {
        return reports.stream().map(report -> report.record.key() + " on " + report.thread).collect(Collectors.toList());
    }

    List<Long> backoffs() {
        return List.copyOf(backoffs);
    }

    int redeliveries() {
        return backoffs.size();
    }

    /** Makes the errant record reporter throw, as it does once {@code errors.tolerance=none} is exceeded. */
    ConnectWorker failingReports(RuntimeException failure) {
        this.reportFailure = failure;
        return this;
    }

    /** Leaves the errant record reporter unset, as without a dead letter queue or error log. */
    ConnectWorker withoutReporter() {
        this.reporterAccess = ReporterAccess.ABSENT;
        return this;
    }

    /** Fails asking for the errant record reporter, as a Connect runtime older than 2.6 does. */
    ConnectWorker withoutReporterSupport(Error error) {
        this.reporterAccess = ReporterAccess.UNSUPPORTED;
        this.runtimeWithoutReporter = error;
        return this;
    }

    private long takeBackoffMs() {
        long backoff = pendingBackoffMs;
        pendingBackoffMs = 0;
        return backoff;
    }

    private Future<Void> report(SinkRecord record, Throwable error) {
        RuntimeException failure = reportFailure;
        if (failure != null) {
            throw failure;
        }
        reports.add(new Report(record, error, Thread.currentThread().getName()));
        return CompletableFuture.completedFuture(null);
    }

    private final class Context implements SinkTaskContext {
        @Override
        public void timeout(long timeoutMs) {
            backoffs.add(timeoutMs);
            pendingBackoffMs = timeoutMs;
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            switch (reporterAccess) {
                case ABSENT:
                    return null;
                case UNSUPPORTED:
                    throw runtimeWithoutReporter;
                default:
                    return ConnectWorker.this::report;
            }
        }

        @Override
        public Map<String, String> configs() {
            return Map.of();
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
        }

        @Override
        public Set<TopicPartition> assignment() {
            return Set.of();
        }

        @Override
        public void pause(TopicPartition... partitions) {
        }

        @Override
        public void resume(TopicPartition... partitions) {
        }

        @Override
        public void requestCommit() {
        }
    }
}
