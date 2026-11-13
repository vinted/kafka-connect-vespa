package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class InvalidMessageHandlerTest {
    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    private final SinkRecord record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "1", Schema.STRING_SCHEMA, "{}", 7);
    private final DataException invalid = new DataException("Cannot convert value 'secret-value'");

    @Test
    void returnsConvertedRecord() {
        assertEquals(Optional.of("converted"), handler(false).convert(record, r -> "converted"));
        verify(reporter, never()).report(any(), any());
    }

    @Test
    void reportsAndDropsInvalidMessagesWhenConfigured() {
        assertEquals(Optional.empty(), handler(true).convert(record, this::fail));
        verify(reporter).report(record, invalid);
    }

    @Test
    void reportsAndFailsOnInvalidMessagesByDefault() {
        assertThrows(DataException.class, () -> handler(false).convert(record, this::fail));

        verify(reporter).report(record, invalid);
    }

    @Test
    void keepsRecordContentOutOfTheFailure() {
        DataException thrown = assertThrows(DataException.class, () -> handler(false).convert(record, this::fail));

        assertEquals("Can't convert record from topic=topic partition=0 offset=7. Failure type: "
                + DataException.class.getName(), thrown.getMessage());
        assertNull(thrown.getCause());
    }

    @Test
    void dropsInvalidMessagesWithoutErrantRecordReporter() {
        InvalidMessageHandler handler = new InvalidMessageHandler(config(true), null);

        assertEquals(Optional.empty(), handler.convert(record, this::fail));
    }

    @Test
    void leavesOtherConversionFailuresAlone() {
        IllegalStateException bug = new IllegalStateException("bug");

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> handler(true).convert(record, r -> { throw bug; }));

        assertSame(bug, thrown);
        verify(reporter, never()).report(any(), any());
    }

    private String fail(SinkRecord record) {
        throw invalid;
    }

    private InvalidMessageHandler handler(boolean dropInvalidMessage) {
        return new InvalidMessageHandler(config(dropInvalidMessage), reporter);
    }

    private static VespaSinkConfig config(boolean dropInvalidMessage) {
        return new VespaSinkConfig(Map.of(VespaSinkConfig.DROP_INVALID_MESSAGE_CONFIG, String.valueOf(dropInvalidMessage)));
    }
}
