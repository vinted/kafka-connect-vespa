package com.vinted.kafka.connect.vespa.mocks;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.OperationStats;
import ai.vespa.feed.client.Result;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Records each operation sent, in order, and answers it as scripted for its document, or else for every document.
 * {@link #acknowledgingAll()} instead succeeds every operation at once and records nothing, to measure what the
 * connector itself costs.
 */
public final class FakeFeedClient implements FeedClient {
    private static final CompletableFuture<Result> ACKNOWLEDGED = CompletableFuture.completedFuture(resultOf(Result.Type.success, null));

    private final Executor answers;
    private final boolean acknowledgeAll;
    private final List<Operation> operations = new CopyOnWriteArrayList<>();
    private final Map<DocumentId, Answer> answersByDocument = new ConcurrentHashMap<>();
    private volatile Answer answer = Answer.success();
    private volatile CircuitBreaker.State circuitBreakerState = CircuitBreaker.State.CLOSED;
    private final AtomicInteger closes = new AtomicInteger();

    /** Answers each operation before returning its future. */
    public FakeFeedClient() {
        this(Runnable::run);
    }

    /** Answers each operation by running the answer on {@code answers}, as the real client does on its own threads. */
    public FakeFeedClient(Executor answers) {
        this(answers, false);
    }

    private FakeFeedClient(Executor answers, boolean acknowledgeAll) {
        this.answers = answers;
        this.acknowledgeAll = acknowledgeAll;
    }

    public static FakeFeedClient acknowledgingAll() {
        return new FakeFeedClient(Runnable::run, true);
    }

    /** A result of the given type for the document, as the feed client completes an operation with. */
    public static Result resultOf(Result.Type type, DocumentId documentId) {
        return new ScriptedResult(type, documentId);
    }

    /** Answers every document without an answer of its own; success unless scripted. */
    public FakeFeedClient answerAll(Answer answer) {
        this.answer = answer;
        return this;
    }

    public FakeFeedClient answer(String documentId, Answer answer) {
        answersByDocument.put(DocumentId.of(documentId), answer);
        return this;
    }

    public void circuitBreakerState(CircuitBreaker.State state) {
        this.circuitBreakerState = state;
    }

    /** The operations sent so far, in the order they were sent. */
    public List<Operation> operations() {
        return List.copyOf(operations);
    }

    public boolean isClosed() {
        return closes() > 0;
    }

    public int closes() {
        return closes.get();
    }

    @Override
    public CompletableFuture<Result> put(DocumentId documentId, String documentJson, OperationParameters params) {
        return send(new Operation(Operation.Kind.PUT, documentId, documentJson, params));
    }

    @Override
    public CompletableFuture<Result> update(DocumentId documentId, String updateJson, OperationParameters params) {
        return send(new Operation(Operation.Kind.UPDATE, documentId, updateJson, params));
    }

    @Override
    public CompletableFuture<Result> remove(DocumentId documentId, OperationParameters params) {
        return send(new Operation(Operation.Kind.REMOVE, documentId, null, params));
    }

    @Override
    public OperationStats stats() {
        return null;
    }

    @Override
    public void resetStats() {
    }

    @Override
    public CircuitBreaker.State circuitBreakerState() {
        return circuitBreakerState;
    }

    @Override
    public void close(boolean graceful) {
        closes.incrementAndGet();
    }

    private CompletableFuture<Result> send(Operation operation) {
        if (acknowledgeAll) {
            return ACKNOWLEDGED;
        }
        Answer scripted = answersByDocument.getOrDefault(operation.documentId, answer);
        if (scripted.refusal != null) {
            throw scripted.refusal;
        }
        operations.add(operation);
        CompletableFuture<Result> future = new CompletableFuture<>();
        answers.execute(() -> scripted.complete(future, operation.documentId));
        return future;
    }

    /** An operation as the feed client received it; the JSON is {@code null} for a remove. */
    public static final class Operation {
        public enum Kind {
            PUT,
            UPDATE,
            REMOVE
        }

        public final Kind kind;
        public final DocumentId documentId;
        public final String json;
        public final OperationParameters parameters;

        private Operation(Kind kind, DocumentId documentId, String json, OperationParameters parameters) {
            this.kind = kind;
            this.documentId = documentId;
            this.json = json;
            this.parameters = parameters;
        }

        /** The kind, document id and JSON, e.g. {@code put id:ns:dt::1 {"fields":{}}}, without the parameters. */
        @Override
        public String toString() {
            String operation = kind.name().toLowerCase() + " " + documentId;
            return json == null ? operation : operation + " " + json;
        }
    }

    /** A result of the given type, a failure, or a refusal to take the operation at all, for an operation. */
    public static final class Answer {
        private final Result.Type type;
        private final Throwable failure;
        private final RuntimeException refusal;

        private Answer(Result.Type type, Throwable failure, RuntimeException refusal) {
            this.type = type;
            this.failure = failure;
            this.refusal = refusal;
        }

        public static Answer success() {
            return result(Result.Type.success);
        }

        public static Answer result(Result.Type type) {
            return new Answer(type, null, null);
        }

        public static Answer failure(Throwable failure) {
            return new Answer(null, failure, null);
        }

        /** Throws from the call that sends the operation, as a closed client does, so the operation is not sent. */
        public static Answer refusal(RuntimeException refusal) {
            return new Answer(null, null, refusal);
        }

        private void complete(CompletableFuture<Result> future, DocumentId documentId) {
            if (failure != null) {
                future.completeExceptionally(failure);
                return;
            }
            future.complete(resultOf(type, documentId));
        }
    }

    private static final class ScriptedResult implements Result {
        private final Type type;
        private final DocumentId documentId;

        private ScriptedResult(Type type, DocumentId documentId) {
            this.type = type;
            this.documentId = documentId;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public DocumentId documentId() {
            return documentId;
        }

        @Override
        public Optional<String> resultMessage() {
            return Optional.empty();
        }

        @Override
        public Optional<String> traceMessage() {
            return Optional.empty();
        }

        @Override
        public String toString() {
            return "Result{type=" + type + ", documentId=" + documentId + "}";
        }
    }
}
