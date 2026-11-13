package com.vinted.kafka.connect.vespa.feeders;

import ai.vespa.feed.client.DocumentId;
import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.OperationParameters;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import com.vinted.kafka.connect.vespa.mocks.RecordingFeedClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FeedSessionTest {
    private static final DocumentId ID = DocumentId.of("id:ns:dt::a");
    private static final String DOCUMENT_JSON = "{\"put\":\"id:ns:dt::a\",\"fields\":{}}";

    private final RecordingFeedClient client = new RecordingFeedClient();

    @Test
    void closesTheClientOnceWhenOnlyDocumentOperationsWereSent() throws IOException {
        FeedSession session = new FeedSession(client, OperationParameters.empty());
        session.put(ID, "{\"fields\":{}}", OperationParameters.empty()).join();
        session.remove(ID, OperationParameters.empty()).join();

        session.close();

        assertEquals(1, client.closes());
    }

    @Test
    void closesTheClientOnceAfterSendingDocumentJsonOperations() throws IOException {
        FeedSession session = new FeedSession(client, OperationParameters.empty());
        session.feed(DOCUMENT_JSON).join();
        session.feed(DOCUMENT_JSON).join();

        session.close();

        assertEquals(1, client.closes());
    }

    @Test
    void sendsDocumentJsonOperationsWithTimeoutRouteAndTracelevelButNotConditionOrCreate() {
        VespaSinkConfig config = new VespaSinkConfig(Map.of(
                VespaSinkConfig.OPERATION_TIMEOUT_MS_CONFIG, "20",
                VespaSinkConfig.OPERATION_ROUTE_CONFIG, "custom",
                VespaSinkConfig.OPERATION_TRACELEVEL_CONFIG, "3",
                VespaSinkConfig.OPERATION_CONDITION_CONFIG, "dt.locked == false",
                VespaSinkConfig.OPERATION_CREATE_IF_NON_EXISTENT_CONFIG, "true"));
        FeedSession session = new FeedSession(client, config.operationParameters());

        session.feed(DOCUMENT_JSON).join();

        OperationParameters expected = OperationParameters.empty()
                .timeout(Duration.ofMillis(20))
                .route("custom")
                .tracelevel(3);
        assertEquals(expected, client.operations().get(0).parameters);
    }

    @Test
    void isBrokenOnceTheCircuitBreakerOpens() {
        FeedSession session = new FeedSession(client, OperationParameters.empty());
        client.circuitBreakerState(FeedClient.CircuitBreaker.State.HALF_OPEN);
        assertFalse(session.isBroken());

        client.circuitBreakerState(FeedClient.CircuitBreaker.State.OPEN);

        assertTrue(session.isBroken());
    }
}
