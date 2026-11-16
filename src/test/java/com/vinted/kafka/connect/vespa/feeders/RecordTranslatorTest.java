package com.vinted.kafka.connect.vespa.feeders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentJson;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class RecordTranslatorTest {
  private static final DataException INVALID =
      new DataException("Cannot convert value 'secret-value'");
  private static final IllegalStateException BUG = new IllegalStateException("bug");

  private final ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
  private long offset = 7;

  @Test
  void keepsRecordContentOutOfTheFailure() {
    VespaFeedException thrown =
        assertThrows(
            VespaFeedException.class,
            () -> translator(false).operations(List.of(record("invalid"))));

    assertEquals(
        "invalid message: DataException: Can't convert record from topic=topic partition=0 "
            + "offset=7. Failure type: "
            + DataException.class.getName(),
        thrown.getMessage());
    assertNull(assertInstanceOf(DataException.class, thrown.getCause()).getCause());
  }

  @Test
  void leavesOtherTranslationFailuresAlone() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> translator(true).operations(List.of(record("bug"))));

    assertSame(BUG, thrown);
    verify(reporter, never()).report(any(), any());
  }

  private RecordTranslator<DocumentJson> translator(boolean dropInvalidMessage) {
    return new RecordTranslator<>(dropInvalidMessage, reporter) {
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
