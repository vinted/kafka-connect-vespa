package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.Result;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Turns records into Vespa operations in one operational mode. Closing it closes its feed client. */
interface VespaFeeder extends Closeable {
    /** Sends an operation per record without waiting for them. */
    List<CompletableFuture<Result>> feed(Collection<SinkRecord> records);
}
