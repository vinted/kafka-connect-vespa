package com.vinted.kafka.connect.vespa.mocks;

import ai.vespa.feed.client.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MockVespaFeedClient implements FeedClient {
    final Map<DocumentId, String> allOperations = new LinkedHashMap<>();
    final Map<DocumentId, String> putOperations = new LinkedHashMap<>();
    final Map<DocumentId, String> updateOperations = new LinkedHashMap<>();
    final Map<DocumentId, String> removeOperations = new LinkedHashMap<>();

    @Override
    public CompletableFuture<Result> put(DocumentId documentId, String documentJson, OperationParameters params) {
        allOperations.put(documentId, documentJson);
        putOperations.put(documentId, documentJson);
        return createSuccessResult(documentId);
    }

    @Override
    public CompletableFuture<Result> update(DocumentId documentId, String updateJson, OperationParameters params) {
        allOperations.put(documentId, updateJson);
        updateOperations.put(documentId, updateJson);
        return createSuccessResult(documentId);
    }

    @Override
    public CompletableFuture<Result> remove(DocumentId documentId, OperationParameters params) {
        allOperations.put(documentId, null);
        removeOperations.put(documentId, null);
        return createSuccessResult(documentId);
    }

    @Override
    public OperationStats stats() {
        return null;
    }

    @Override
    public CircuitBreaker.State circuitBreakerState() {
        return null;
    }

    @Override
    public void close(boolean graceful) {
    }

    public void assertDocumentIds(Collection<DocumentId> keys, String... expectedUserSpecificIds) {
        List<String> expected = Arrays.stream(expectedUserSpecificIds)
                .sorted()
                .collect(Collectors.toList());
        List<String> actual = keys.stream()
                .map(DocumentId::toString).sorted()
                .collect(Collectors.toList());
        assertEquals(expected, actual, "Document ids must match");
    }

    public void assertAllDocumentIds(String... expectedUserSpecificIds) {
        assertDocumentIds(allOperations.keySet(), expectedUserSpecificIds);
    }

    public void assertPutDocumentIds(String... expectedUserSpecificIds) {
        assertDocumentIds(putOperations.keySet(), expectedUserSpecificIds);
    }

    public void assertUpdateDocumentIds(String... expectedUserSpecificIds) {
        assertDocumentIds(updateOperations.keySet(), expectedUserSpecificIds);
    }

    public void assertRemoveDocumentIds(String... expectedUserSpecificIds) {
        assertDocumentIds(removeOperations.keySet(), expectedUserSpecificIds);
    }

    public void assertPutOperation(String userSpecificId, String expectedJson) {
        DocumentId docId = DocumentId.of(userSpecificId);
        String json = putOperations.get(docId);
        assertNotNull(json);
        assertEquals(expectedJson.trim(), json.trim());
    }

    public void assertUpdateOperation(String userSpecificId, String expectedJson) {
        DocumentId docId = DocumentId.of(userSpecificId);
        String json = updateOperations.get(docId);
        assertNotNull(json);
        assertEquals(expectedJson.trim(), json.trim());
    }

    private CompletableFuture<Result> createSuccessResult(DocumentId documentId) {
        return CompletableFuture.completedFuture(new Result() {
            @Override
            public Type type() {
                return Type.success;
            }

            @Override
            public DocumentId documentId() {
                return documentId;
            }

            @Override
            public Optional<String> resultMessage() {
                return Optional.of("success");
            }

            @Override
            public Optional<String> traceMessage() {
                return Optional.empty();
            }
        });
    }
}
