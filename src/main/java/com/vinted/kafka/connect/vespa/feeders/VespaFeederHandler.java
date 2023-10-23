package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParseException;
import ai.vespa.feed.client.Result;
import com.fasterxml.jackson.core.JsonParseException;
import com.vinted.kafka.connect.vespa.VespaReporter;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class VespaFeederHandler {
    private final Logger log;
    private final VespaSinkConfig config;
    private final VespaReporter reporter;

    public VespaFeederHandler(Logger log, VespaSinkConfig config, VespaReporter reporter) {
        this.log = log;
        this.config = config;
        this.reporter = reporter;
    }

    public CompletableFuture<Result> handle(SinkRecord record, CompletableFuture<Result> future) {
        CompletableFuture<Result> promise = new CompletableFuture<>();

        future.whenComplete((result, throwable) -> {
            if (result == null) {
                reporter.report(record, throwable);

                if (!isMalformed(throwable)) {
                    log.error(errorMessage(record), throwable);

                    promise.completeExceptionally(throwable);
                } else {
                    switch (config.behaviorOnMalformedDoc) {
                        case IGNORE:
                            log.info(ignoreMessage(record), throwable);
                            promise.complete(ignoredResult());
                            break;
                        case WARN:
                            log.warn(ignoreMessage(record), throwable);
                            promise.complete(ignoredResult());
                            break;
                        case FAIL:
                        default:
                            log.error(errorMessage(record), throwable);

                            promise.completeExceptionally(throwable);
                    }
                }
            } else if (result.type() == Result.Type.success) {
                promise.complete(result);
            } else {
                Throwable resultThrowable = new Throwable(result.toString());

                reporter.report(record, resultThrowable);

                switch (config.behaviorOnMalformedDoc) {
                    case IGNORE:
                        log.info(ignoreMessage(record), throwable);
                        promise.complete(result);
                        break;
                    case WARN:
                        log.warn(ignoreMessage(record), throwable);
                        promise.complete(result);
                        break;
                    case FAIL:
                    default:
                        log.error(errorMessage(record), throwable);

                        promise.completeExceptionally(throwable);
                }
            }
        });

        return promise;
    }

    private static String ignoreMessage(SinkRecord record) {
        return String.format("Failed to index document '%s'. Ignoring and will not index it.", record);
    }

    private static String errorMessage(SinkRecord record) {
        return String.format(
                "Failed to index document '%s'. To ignore future document like this, change the configuration '%s' to '%s'.",
                record,
                VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
                VespaSinkConfig.BehaviorOnMalformedDoc.IGNORE
        );
    }

    private static boolean isMalformed(Throwable throwable) {
        Throwable rootCause = Stream
                .iterate(throwable, Throwable::getCause)
                .filter(element -> element.getCause() == null)
                .findFirst()
                .orElse(throwable);

        String rootCauseString = rootCause.toString().toLowerCase();

        return rootCauseString.contains("status 400")
                || rootCauseString.contains("string field value contains illegal code point")
                || rootCause instanceof OperationParseException
                || rootCause instanceof JsonParseException;
    }

    private static Result ignoredResult() {
        return new Result() {
            @Override
            public Type type() {
                return Type.success;
            }

            @Override
            public DocumentId documentId() {
                return null;
            }

            @Override
            public Optional<String> resultMessage() {
                return Optional.empty();
            }

            @Override
            public Optional<String> traceMessage() {
                return Optional.empty();
            }
        };
    }
}
