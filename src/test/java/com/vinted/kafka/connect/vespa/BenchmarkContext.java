package com.vinted.kafka.connect.vespa;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The part of a Connect worker's context a sink task uses: the backoff it asks for before a redelivery, and an errant
 * record reporter that counts the records reported to it.
 */
final class BenchmarkContext implements SinkTaskContext {
    private final AtomicInteger reported = new AtomicInteger();
    private final AtomicInteger redeliveries = new AtomicInteger();
    private volatile long timeoutMs;

    int reported() {
        return reported.get();
    }

    int redeliveries() {
        return redeliveries.get();
    }

    long takeTimeoutMs() {
        long requested = timeoutMs;
        timeoutMs = 0;
        return requested;
    }

    @Override
    public void timeout(long timeoutMs) {
        redeliveries.incrementAndGet();
        this.timeoutMs = timeoutMs;
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

    @Override
    public ErrantRecordReporter errantRecordReporter() {
        return (record, error) -> {
            reported.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        };
    }
}
