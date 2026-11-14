package com.vinted.kafka.connect.vespa.feeders;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Jittered exponential backoff, using the same formula as kafka-connect-elasticsearch's {@code retry.backoff.ms}:
 * a random wait between zero and {@code initialBackoff * 2^retryAttempt}, capped at a day.
 */
final class Backoff {
    static final long MAX_BACKOFF_MS = Duration.ofDays(1).toMillis();

    private Backoff() {
    }

    static long randomBackoffMs(int retryAttempt, long initialBackoffMs) {
        long maxBackoffMs = maxBackoffMs(retryAttempt, initialBackoffMs);
        return maxBackoffMs <= 0 ? 0 : ThreadLocalRandom.current().nextLong(0, maxBackoffMs);
    }

    static long maxBackoffMs(int retryAttempt, long initialBackoffMs) {
        if (initialBackoffMs <= 0) {
            return 0;
        }
        if (retryAttempt <= 0) {
            return initialBackoffMs;
        }
        if (retryAttempt > 32) {
            return MAX_BACKOFF_MS;
        }
        long backoffMs = initialBackoffMs << retryAttempt;
        return backoffMs < 0 ? MAX_BACKOFF_MS : Math.min(MAX_BACKOFF_MS, backoffMs);
    }
}
