package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.OperationParseException;
import ai.vespa.feed.client.Result;
import com.fasterxml.jackson.core.JsonParseException;
import com.vinted.kafka.connect.vespa.VespaReporter;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.stream.Stream;

public class VespaResultCallbackHandler {
    private final Logger log;
    private final VespaSinkConfig config;
    private final VespaReporter reporter;

    public VespaResultCallbackHandler(Logger log, VespaSinkConfig config, VespaReporter reporter) {
        this.log = log;
        this.config = config;
        this.reporter = reporter;
    }

    public Result handle(SinkRecord record, Result result, Throwable throwable) {
        if (result == null) {
            return handleThrowable(record, throwable);
        } else {
            return handleResult(record, result);
        }
    }

    private Result handleResult(SinkRecord record, Result result) {
        if (result.type() == Result.Type.success) {
            return result;
        }

        return handleMalformed(record, result, new ConnectException(result.toString()));
    }

    private Result handleThrowable(SinkRecord record, Throwable throwable) {
        Throwable rootCause = Stream
                .iterate(throwable, Throwable::getCause)
                .filter(element -> element.getCause() == null)
                .findFirst()
                .orElse(throwable);

        Result success = new Result() {
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

        boolean isMalformed = rootCause.toString().toLowerCase().contains("status 400")
                || rootCause instanceof OperationParseException
                || rootCause instanceof JsonParseException;

        if (isMalformed) {
            return handleMalformed(record, success, throwable);
        }

        log.error(errorMessage(record, success), throwable);

        throw new ConnectException(throwable);
    }

    private Result handleMalformed(SinkRecord record, Result result, Throwable throwable) {
        reporter.report(record, throwable);

        switch (config.behaviorOnMalformedDoc) {
            case IGNORE:
                log.debug(ignoreMessage(record), throwable);
                return result;
            case WARN:
                log.warn(ignoreMessage(record), throwable);
                return result;
            case FAIL:
            default:
                log.error(errorMessage(record, result), throwable);
                throw new ConnectException(throwable);
        }
    }

    private String ignoreMessage(SinkRecord record) {
        return String.format("Failed to index document '%s'. Ignoring and will not index it.", record);
    }

    private String errorMessage(SinkRecord record, Result result) {
        return String.format(
                "Failed to index document '%s'. ResultMessage: '%s'. Trace: '%s'. "
                        + "To ignore future document like this, change the configuration '%s' to '%s'.",
                record,
                result.resultMessage(),
                result.traceMessage(),
                VespaSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
                VespaSinkConfig.BehaviorOnMalformedDoc.IGNORE
        );
    }
}
