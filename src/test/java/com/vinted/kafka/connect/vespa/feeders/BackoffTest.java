package com.vinted.kafka.connect.vespa.feeders;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackoffTest {
    @Test
    void doublesTheUpperBoundEachRetry() {
        assertEquals(1000, Backoff.maxBackoffMs(0, 1000));
        assertEquals(2000, Backoff.maxBackoffMs(1, 1000));
        assertEquals(4000, Backoff.maxBackoffMs(2, 1000));
        assertEquals(1024000, Backoff.maxBackoffMs(10, 1000));
    }

    @Test
    void capsTheUpperBoundAtADay() {
        assertEquals(Backoff.MAX_BACKOFF_MS, Backoff.maxBackoffMs(30, 1000));
        assertEquals(Backoff.MAX_BACKOFF_MS, Backoff.maxBackoffMs(33, 1000));
        assertEquals(Backoff.MAX_BACKOFF_MS, Backoff.maxBackoffMs(20, Long.MAX_VALUE / 4));
    }

    @Test
    void retriesAtOnceWithoutBackoff() {
        assertEquals(0, Backoff.randomBackoffMs(3, 0));
    }

    @RepeatedTest(20)
    void staysWithinTheUpperBound() {
        long backoffMs = Backoff.randomBackoffMs(3, 1000);

        assertTrue(backoffMs >= 0 && backoffMs < 8000, "backoff " + backoffMs);
    }
}
