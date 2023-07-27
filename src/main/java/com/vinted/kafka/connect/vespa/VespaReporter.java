package com.vinted.kafka.connect.vespa;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

public class VespaReporter {
    private final ErrantRecordReporter reporter;

    public VespaReporter(ErrantRecordReporter reporter) {
        this.reporter = reporter;
    }

    public void report(SinkRecord record, Throwable throwable) {
        if (reporter != null) {
            reporter.report(record, throwable);
        }
    }
}
