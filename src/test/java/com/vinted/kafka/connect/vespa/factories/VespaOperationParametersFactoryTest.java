package com.vinted.kafka.connect.vespa.factories;

import ai.vespa.feed.client.OperationParameters;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VespaOperationParametersFactoryTest {
    Map<String, String> params;

    @BeforeEach
    void setup() {
        params = new HashMap<>();
    }

    @Test
    void defaultsToMinuteTimeout() {
        OperationParameters subject = VespaOperationParametersFactory.create(new VespaSinkConfig(params));

        OperationParameters expected = OperationParameters.empty().timeout(Duration.ofMinutes(1));

        assertEquals(expected, subject);
    }

    @Test
    void overridesParams() {
        params.put("vespa.operation.timeout.ms", "20");
        params.put("vespa.operation.tracelevel", "3");
        params.put("vespa.operation.route", "custom");

        OperationParameters subject = VespaOperationParametersFactory.create(new VespaSinkConfig(params));

        OperationParameters expected = OperationParameters
                .empty()
                .timeout(Duration.ofMillis(20))
                .route("custom")
                .tracelevel(3);

        assertEquals(expected, subject);
    }
}
