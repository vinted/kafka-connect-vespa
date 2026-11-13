package com.vinted.kafka.connect.vespa.mocks;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class FeedAssertions {
    private FeedAssertions() {
    }

    /** Asserts the client received exactly these operations, in order, as {@link RecordingFeedClient.Operation#toString()} spells them. */
    public static void assertSent(RecordingFeedClient client, String... operations) {
        List<String> sent = client.operations().stream()
                .map(RecordingFeedClient.Operation::toString)
                .collect(Collectors.toList());
        assertEquals(List.of(operations), sent, "operations sent to the feed client");
    }

    public static void assertNothingSent(RecordingFeedClient client) {
        assertSent(client);
    }
}
