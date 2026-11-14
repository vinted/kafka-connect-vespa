package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.Result;
import org.apache.kafka.connect.sink.SinkRecord;
import com.vinted.kafka.connect.vespa.mocks.MockVespaServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Feeds generated records through {@link VespaSinkTask} into a running Vespa, the way a Connect worker does, and
 * reports throughput, put latency and CPU. Run against the application in {@code src/test/resources/performance/app}
 * with {@code scripts/performance.sh}, or with {@code -Dvespa.perf.endpoint=...} against one already running. With
 * {@code -Dvespa.perf.endpoint=mock} it feeds a local server that acknowledges every operation at once instead, which
 * measures the connector and the feed client alone, and with {@code -Dvespa.perf.endpoint=stub} the task gets a
 * feed client that succeeds every operation in memory, which measures the connector alone.
 */
@Tag("performance")
class FeedPerformanceTest {
    private static final String ENDPOINT = System.getProperty("vespa.perf.endpoint");
    private static final int RECORDS = Integer.getInteger("vespa.perf.records", 200_000);
    private static final int WARMUP_RECORDS = Integer.getInteger("vespa.perf.warmup.records", 20_000);
    private static final int BATCH = Integer.getInteger("vespa.perf.batch", 500);
    private static final boolean STUB = "stub".equals(ENDPOINT);
    // Feeding the same records again only makes sense where nothing is indexed.
    private static final int PASSES = STUB ? Integer.getInteger("vespa.perf.passes", 1) : 1;

    private String endpoint;
    private VespaAdmin vespa;
    private MockVespaServer mock;

    @BeforeEach
    void before() throws Exception {
        assumeTrue(ENDPOINT != null, "set vespa.perf.endpoint to run the feed benchmark");

        if (STUB) {
            endpoint = "http://localhost:1";
            return;
        }

        if ("mock".equals(ENDPOINT)) {
            mock = MockVespaServer.http(new AcknowledgingHandler(Long.getLong("vespa.perf.mock.latency.micros", 0)));
            endpoint = mock.url();
            return;
        }

        endpoint = ENDPOINT;
        vespa = new VespaAdmin(endpoint);
        vespa.deleteAll();
    }

    @AfterEach
    void after() throws Exception {
        if (mock != null) {
            mock.close();
        }
    }

    @Test
    void upsertJsonStrings() throws Exception {
        run("UPSERT JSON strings", Map.of(), PerformanceRecords::jsonString);
    }

    @Test
    void upsertStructs() throws Exception {
        run("UPSERT structs", Map.of(), PerformanceRecords::struct);
    }

    @Test
    void rawDocumentJson() throws Exception {
        run("RAW document JSON", Map.of(VespaSinkConfig.OPERATIONAL_MODE_CONFIG, "RAW"), PerformanceRecords::rawPut);
    }

    @Test
    void feedClientAlone() throws Exception {
        assumeTrue(!STUB, "the stub replaces the feed client");
        VespaSinkConfig config = new VespaSinkConfig(PerformanceRecords.connectorProps(endpoint, Map.of()));
        List<PerformanceRecords.Document> documents = PerformanceRecords.documents(WARMUP_RECORDS + RECORDS);

        try (FeedClient client = config.feedClient()) {
            putAll(client, config, documents.subList(0, WARMUP_RECORDS));
            clear();

            Measurement measurement = Measurement.start();
            putAll(client, config, documents.subList(WARMUP_RECORDS, documents.size()));
            measurement.stop("feed client alone", RECORDS, List.of());
        }

        assertIndexed(RECORDS);
    }

    private void run(String name, Map<String, String> overrides, PerformanceRecords.Factory factory) throws Exception {
        List<PerformanceRecords.Document> documents = PerformanceRecords.documents(WARMUP_RECORDS + RECORDS);
        List<SinkRecord> warmup = factory.records(documents.subList(0, WARMUP_RECORDS), 0);
        List<SinkRecord> measured = factory.records(documents.subList(WARMUP_RECORDS, documents.size()), WARMUP_RECORDS);

        VespaSinkTask task = STUB ? new VespaSinkTask(config -> new AcknowledgingFeedClient()) : new VespaSinkTask();
        ConnectWorker worker = new ConnectWorker(task).start(PerformanceRecords.connectorProps(endpoint, overrides));

        try {
            worker.deliver(warmup, BATCH);
            clear();

            Measurement measurement = Measurement.start();
            List<Long> latencies = new ArrayList<>();
            for (int pass = 0; pass < PASSES; pass++) {
                latencies.addAll(worker.deliver(measured, BATCH));
            }
            measurement.stop(name, RECORDS * PASSES, latencies);
        } finally {
            worker.stop();
        }

        assertIndexed(RECORDS);
    }

    private void clear() throws Exception {
        if (vespa != null) {
            vespa.deleteAll();
        }
    }

    private void assertIndexed(int records) throws Exception {
        if (vespa == null || Boolean.getBoolean("vespa.perf.connector.vespa.dryrun") || Boolean.getBoolean("vespa.perf.connector.vespa.speed.test")) {
            return;
        }

        assertEquals(records, vespa.count());
    }

    /** Puts every document with all of them in flight, sending those that failed again, as the task's retries do. */
    private static void putAll(FeedClient client, VespaSinkConfig config, List<PerformanceRecords.Document> documents) {
        List<PerformanceRecords.Document> pending = documents;
        while (!pending.isEmpty()) {
            List<CompletableFuture<Result>> results = new ArrayList<>(pending.size());
            for (PerformanceRecords.Document document : pending) {
                results.add(client.put(DocumentId.of("perf", "item", document.key),
                        "{\"fields\":" + document.json + "}", config.putParameters()));
            }

            List<PerformanceRecords.Document> failed = new ArrayList<>();
            for (int i = 0; i < pending.size(); i++) {
                if (results.get(i).handle((result, failure) -> failure != null).join()) {
                    failed.add(pending.get(i));
                }
            }
            pending = failed;
        }
    }

    /** Wall time, process CPU and put latencies of one measured feed. */
    static final class Measurement {
        private final long startNanos = System.nanoTime();
        private final long startCpuNanos = processCpuNanos();

        static Measurement start() {
            return new Measurement();
        }

        void stop(String name, int records, List<Long> latencies) {
            double seconds = (System.nanoTime() - startNanos) / 1e9;
            double cpuSeconds = (processCpuNanos() - startCpuNanos) / 1e9;
            long[] sorted = latencies.stream().mapToLong(Long::longValue).sorted().toArray();

            System.out.printf("%n[performance] %-22s %,9.0f docs/s  %6.1f s  cpu %5.1f s (%.1f cores, %.0f us/doc)"
                            + "  put p50 %s p99 %s max %s ms  batch %d%n",
                    name, records / seconds, seconds, cpuSeconds, cpuSeconds / seconds, cpuSeconds * 1e6 / records,
                    millis(sorted, 0.50), millis(sorted, 0.99), millis(sorted, 1.0), BATCH);
        }

        private static String millis(long[] sorted, double quantile) {
            if (sorted.length == 0) {
                return "-";
            }
            int index = Math.min(sorted.length - 1, (int) Math.ceil(quantile * sorted.length) - 1);
            return String.format("%.1f", sorted[Math.max(0, index)] / 1e6);
        }

        private static long processCpuNanos() {
            return ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime();
        }
    }
}
