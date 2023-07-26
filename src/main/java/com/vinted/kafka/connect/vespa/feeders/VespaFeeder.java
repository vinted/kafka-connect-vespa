package com.vinted.kafka.connect.vespa.feeders;

import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;
import java.util.Collection;

public interface VespaFeeder extends Closeable {
    void feed(Collection<SinkRecord> collection);
}
