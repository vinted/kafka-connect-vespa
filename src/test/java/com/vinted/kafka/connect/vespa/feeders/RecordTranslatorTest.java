package com.vinted.kafka.connect.vespa.feeders;

import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentJson;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RecordTranslatorTest {
    private static final DataException INVALID = new DataException("Cannot convert value 'secret-value'");
    private static final IllegalStateException BUG = new IllegalStateException("bug");

    private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    private long offset = 7;

    @Test
    void reportsAndDropsInvalidMessagesWhenConfigured() {
        SinkRecord before = record("valid");
        SinkRecord invalid = record("invalid");
        SinkRecord after = record("valid");

        List<DocumentJson> operations = translator(true).operations(List.of(before, invalid, after));

        assertEquals(List.of(before, after), operations.stream().map(operation -> operation.record).collect(Collectors.toList()));
        verify(reporter).report(invalid, INVALID);
    }

    @Test
    void reportsAndFailsOnInvalidMessagesByDefault() {
        SinkRecord invalid = record("invalid");

        assertThrows(VespaFeedException.class, () -> translator(false).operations(List.of(invalid)));

        verify(reporter).report(invalid, INVALID);
    }

    @Test
    void keepsRecordContentOutOfTheFailure() {
        VespaFeedException thrown = assertThrows(VespaFeedException.class,
                () -> translator(false).operations(List.of(record("invalid"))));

        assertEquals("invalid message: DataException: Can't convert record from topic=topic partition=0 offset=7. "
                + "Failure type: " + DataException.class.getName(), thrown.getMessage());
        assertNull(assertInstanceOf(DataException.class, thrown.getCause()).getCause());
    }

    @Test
    void leavesOtherTranslationFailuresAlone() {
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> translator(true).operations(List.of(record("bug"))));

        assertSame(BUG, thrown);
        verify(reporter, never()).report(any(), any());
    }

    private RecordTranslator<DocumentJson> translator(boolean dropInvalidMessage) {
        VespaSinkConfig config = new VespaSinkConfig(
                Map.of(VespaSinkConfig.DROP_INVALID_MESSAGE_CONFIG, String.valueOf(dropInvalidMessage)));

        return new RecordTranslator<>(config, reporter) {
            @Override
            Optional<DocumentJson> translate(SinkRecord record) {
                if ("invalid".equals(record.value())) {
                    throw INVALID;
                }

                if ("bug".equals(record.value())) {
                    throw BUG;
                }

                return Optional.of(new DocumentJson(record, "{}"));
            }

            @Override
            List<DocumentJson> compact(List<DocumentJson> operations) {
                return operations;
            }
        };
    }

    private SinkRecord record(String value) {
        return new SinkRecord("topic", 0, null, null, Schema.STRING_SCHEMA, value, offset++);
    }
}
