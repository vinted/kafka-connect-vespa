package com.vinted.kafka.connect.vespa;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Checks what the connector promises while it feeds a running Vespa at full speed: every valid record lands, every
 * malformed document is reported once, every RAW operation is applied in order, and nothing is lost when Vespa
 * becomes unavailable mid-feed. Runs against the application in {@code src/test/resources/performance/app}.
 */
@Tag("performance")
class FeedUnderLoadTest {
    private static final String ENDPOINT = System.getProperty("vespa.perf.endpoint");
    private static final int RECORDS = Integer.getInteger("vespa.perf.records", 200_000);
    private static final String OUTAGE_COMMAND = System.getProperty("vespa.perf.outage.command");

    private final BenchmarkContext context = new BenchmarkContext();
    private VespaAdmin vespa;

    @BeforeEach
    void before() throws Exception {
        assumeTrue(ENDPOINT != null && ENDPOINT.startsWith("http"), "set vespa.perf.endpoint to a running Vespa");
        vespa = new VespaAdmin(ENDPOINT);
        vespa.deleteAll();
    }

    @Test
    void skipsEveryMalformedDocumentAndIndexesTheRest() throws Exception {
        List<SinkRecord> records = new ArrayList<>(RECORDS);
        int malformed = 0;
        for (PerformanceRecords.Document document : PerformanceRecords.documents(RECORDS)) {
            boolean isMalformed = records.size() % 100 == 0;
            String json = isMalformed ? document.json.replaceFirst("\"views\":\\d+", "\"views\":\"many\"") : document.json;
            records.add(new SinkRecord("items", 0, Schema.STRING_SCHEMA, document.key, Schema.STRING_SCHEMA, json, records.size()));
            malformed += isMalformed ? 1 : 0;
        }

        feed(Map.of(VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "IGNORE"), records);

        assertEquals(RECORDS - malformed, vespa.count());
        assertEquals(malformed, context.reported());
    }

    @Test
    void appliesEveryRawOperationInOrder() throws Exception {
        int documents = RECORDS / 40;
        int increments = 20;
        List<String> operations = new ArrayList<>();
        // The first half of the documents get all their operations in a row, so most share a batch; the rest get
        // theirs round by round, so each lands in a different batch.
        for (int document = 0; document < documents / 2; document++) {
            operations.add(put(document));
            for (int increment = 0; increment < increments; increment++) {
                operations.add(increment(document));
            }
        }
        for (int document = documents / 2; document < documents; document++) {
            operations.add(put(document));
        }
        for (int increment = 0; increment < increments; increment++) {
            for (int document = documents / 2; document < documents; document++) {
                operations.add(increment(document));
            }
        }

        List<SinkRecord> records = new ArrayList<>(operations.size());
        for (String operation : operations) {
            records.add(new SinkRecord("items", 0, null, null, Schema.STRING_SCHEMA, operation, records.size()));
        }

        feed(Map.of(VespaSinkConfig.OPERATIONAL_MODE_CONFIG, "RAW"), records);

        assertEquals(Map.of((long) increments, (long) documents), vespa.countByViews());
    }

    @Test
    void losesNothingWhenVespaRestartsMidFeed() throws Exception {
        assumeTrue(OUTAGE_COMMAND != null, "set vespa.perf.outage.command to a command that restarts Vespa");
        PerformanceRecords.Factory jsonStrings = PerformanceRecords::jsonString;
        List<SinkRecord> records = jsonStrings.records(PerformanceRecords.documents(RECORDS), 0);
        int batch = Integer.getInteger("vespa.perf.batch", 500);
        int outageAt = records.size() / 4 / batch * batch;
        VespaSinkTask task = start(Map.of());
        CompletableFuture<Void> outage = CompletableFuture.completedFuture(null);
        long slowestBatchNanos = 0;

        try {
            for (int from = 0; from < records.size(); from += batch) {
                if (from == outageAt) {
                    outage = CompletableFuture.runAsync(FeedUnderLoadTest::restartVespa);
                }
                long start = System.nanoTime();
                FeedPerformanceTest.deliverBatch(task, context, records.subList(from, Math.min(records.size(), from + batch)));
                slowestBatchNanos = Math.max(slowestBatchNanos, System.nanoTime() - start);
            }
        } finally {
            task.stop();
        }

        outage.join();
        System.out.printf("%n[performance] outage: %d redeliveries, slowest batch %.1f s%n",
                context.redeliveries(), slowestBatchNanos / 1e9);
        assertEquals(RECORDS, vespa.count());
    }

    private void feed(Map<String, String> overrides, List<SinkRecord> records) throws InterruptedException {
        VespaSinkTask task = start(overrides);
        try {
            FeedPerformanceTest.deliver(task, context, records);
        } finally {
            task.stop();
        }
    }

    private VespaSinkTask start(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put(VespaSinkConfig.ENDPOINT_CONFIG, ENDPOINT);
        props.put(VespaSinkConfig.NAMESPACE_CONFIG, "perf");
        props.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "item");
        System.getProperties().stringPropertyNames().stream()
                .filter(name -> name.startsWith("vespa.perf.connector."))
                .forEach(name -> props.put(name.substring("vespa.perf.connector.".length()), System.getProperty(name)));
        props.putAll(overrides);

        VespaSinkTask task = new VespaSinkTask();
        task.initialize(context);
        task.start(props);
        return task;
    }

    private static void restartVespa() {
        try {
            Process process = new ProcessBuilder("sh", "-c", OUTAGE_COMMAND).inheritIO().start();
            assertEquals(0, process.waitFor(), "outage command");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String put(int document) {
        return "{\"put\":\"id:perf:item::" + document + "\",\"fields\":{\"title\":\"item " + document + "\",\"views\":0}}";
    }

    private static String increment(int document) {
        return "{\"update\":\"id:perf:item::" + document + "\",\"fields\":{\"views\":{\"increment\":1}}}";
    }
}
