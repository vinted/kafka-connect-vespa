package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParameters;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentOperation;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Put;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Remove;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * UPSERT: the record key is the document id, within the configured or topic namespace and document type. The value
 * is put as the document's fields; a tombstone removes the document, and an empty value is an invalid message.
 */
final class UpsertTranslator implements RecordTranslator<DocumentOperation> {
    private final VespaSinkConfig config;
    private final OperationParameters putParameters;
    private final OperationParameters removeParameters;

    UpsertTranslator(VespaSinkConfig config) {
        this.config = config;
        this.putParameters = config.putParameters();
        this.removeParameters = config.removeParameters();
    }

    @Override
    public Optional<DocumentOperation> translate(SinkRecord record) {
        String key = KeyConverter.convert(record);
        String value = ValueConverter.convert(record);
        String topic = record.topic();
        DocumentId documentId = DocumentId.of(config.namespace(topic), config.documentType(topic), key);

        if (value == null) {
            return Optional.of(new Remove(record, documentId, removeParameters));
        }

        if (value.isBlank()) {
            throw new DataException("Value is empty; only a null value removes the document.");
        }

        return Optional.of(new Put(record, documentId, String.format("{\"fields\":%s}", value), putParameters));
    }

    /** Keeps only the last operation per document. */
    @Override
    public List<DocumentOperation> compact(List<DocumentOperation> operations) {
        Map<DocumentId, DocumentOperation> latest = new LinkedHashMap<>();
        operations.forEach(operation -> latest.put(operation.documentId, operation));
        return List.copyOf(latest.values());
    }
}
