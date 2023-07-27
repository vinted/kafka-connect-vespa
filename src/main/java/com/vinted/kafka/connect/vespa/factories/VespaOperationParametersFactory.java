package com.vinted.kafka.connect.vespa.factories;

import ai.vespa.feed.client.OperationParameters;
import com.google.common.base.Strings;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;

public class VespaOperationParametersFactory {
    public static OperationParameters create(VespaSinkConfig config) {
        OperationParameters parameters = OperationParameters.empty();

        if (config.timeout.toMillis() > 0) {
            parameters = parameters.timeout(config.timeout);
        }

        if (!Strings.isNullOrEmpty(config.route)) {
            parameters = parameters.route(config.route);
        }

        if (config.tracelevel > 0) {
            parameters = parameters.tracelevel(config.tracelevel);
        }

        return parameters;
    }
}
