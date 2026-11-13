package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParameters;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.converters.KeyConverter;
import com.vinted.kafka.connect.vespa.converters.ValueConverter;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.DocumentOperation;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Put;
import com.vinted.kafka.connect.vespa.feeders.VespaOperation.Remove;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * UPSERT: the record key is the document id, within the configured or topic namespace and document type. The value
 * is put as the document's fields; an empty value removes the document.
 */
final class UpsertTranslator implements RecordTranslator<DocumentOperation> {
    private final KeyConverter keyConverter = new KeyConverter();
    private final ValueConverter valueConverter = new ValueConverter();
    private final String namespace;
    private final String documentType;
    private final OperationParameters putParameters;
    private final OperationParameters removeParameters;

    UpsertTranslator(VespaSinkConfig config) {
        this.namespace = config.namespace;
        this.documentType = config.documentType;
        this.removeParameters = conditional(RecordTranslator.commonParameters(config), config.condition);
        this.putParameters = removeParameters.createIfNonExistent(config.createIfNonExistent);
    }

    @Override
    public Optional<DocumentOperation> translate(SinkRecord record) {
        String key = keyConverter.convert(record);
        String value = valueConverter.convert(record);
        DocumentId documentId = DocumentId.of(orTopic(namespace, record), orTopic(documentType, record), key);

        if (Strings.isNullOrEmpty(value)) {
            return Optional.of(new Remove(record, documentId, removeParameters));
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

    private static OperationParameters conditional(OperationParameters parameters, String condition) {
        return Strings.isNullOrEmpty(condition) ? parameters : parameters.testAndSetCondition(condition);
    }

    private static String orTopic(String configured, SinkRecord record) {
        return configured == null ? record.topic() : configured;
    }
}
