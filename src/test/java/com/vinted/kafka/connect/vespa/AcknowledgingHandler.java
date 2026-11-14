package com.vinted.kafka.connect.vespa;

import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Answers every feed client request, the handshake included, with an empty Vespa JSON success, after the given
 * latency, without holding a thread meanwhile.
 */
final class AcknowledgingHandler extends Handler.Abstract {
    private final long latencyMicros;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    AcknowledgingHandler(long latencyMicros) {
        this.latencyMicros = latencyMicros;
    }

    @Override
    public boolean handle(Request request, Response response, Callback callback) {
        if (latencyMicros <= 0) {
            acknowledge(response, callback);
            return true;
        }

        scheduler.schedule(() -> acknowledge(response, callback), latencyMicros, TimeUnit.MICROSECONDS);
        return true;
    }

    @Override
    protected void doStop() throws Exception {
        scheduler.shutdownNow();
        super.doStop();
    }

    private static void acknowledge(Response response, Callback callback) {
        response.setStatus(200);
        response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json");
        Content.Sink.write(response, true, "{}", callback);
    }
}
