package com.vinted.kafka.connect.vespa.mocks;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A local Vespa {@code /document/v1} endpoint for the real feed client to talk to over HTTP/2. It answers the feed
 * client's handshake and every operation the way it is told, in Vespa's terms: acknowledge, a Vespa error with a
 * status, a body that is not Vespa JSON, or no answer at all.
 */
public final class MockVespaServer implements AutoCloseable {
    private static final String HANDSHAKE_PATH = "/document/v1/feeder/handshake/docid/dummy";
    private static final String JSON = "application/json";
    private static final ScheduledExecutorService LATER = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "mock-vespa-latency");
        thread.setDaemon(true);
        return thread;
    });

    private final Server server;
    private final String scheme;

    private MockVespaServer(Server server, String scheme) {
        this.server = server;
        this.scheme = scheme;
    }

    /** Serves cleartext HTTP/2, which the feed client speaks to an {@code http://} endpoint, acknowledging the handshake. */
    public static MockVespaServer http(Request.Handler operations) throws Exception {
        return http(acknowledge(), operations);
    }

    public static MockVespaServer http(Request.Handler handshake, Request.Handler operations) throws Exception {
        return start("http", handshake, operations, server -> new ServerConnector(
                server, new HTTP2CServerConnectionFactory(new HttpConfiguration())));
    }

    public static MockVespaServer https(SslContextFactory.Server tls, Request.Handler handshake, Request.Handler operations)
            throws Exception {
        HttpConfiguration configuration = new HttpConfiguration();
        configuration.addCustomizer(new SecureRequestCustomizer());
        HTTP2ServerConnectionFactory http2 = new HTTP2ServerConnectionFactory(configuration);
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol(http2.getProtocol());

        return start("https", handshake, operations, server -> new ServerConnector(
                server, new SslConnectionFactory(tls, alpn.getProtocol()), alpn, http2));
    }

    /** Answers 200 with an empty Vespa JSON body, as Vespa acknowledges an operation or the handshake. */
    public static Request.Handler acknowledge() {
        return respond(200, JSON, "{}");
    }

    /** Acknowledges once the latency has passed, without holding a server thread meanwhile. */
    public static Request.Handler acknowledgeAfter(Duration latency) {
        if (latency.isZero()) {
            return acknowledge();
        }

        Request.Handler acknowledge = acknowledge();
        return (request, response, callback) -> {
            LATER.schedule(() -> answer(acknowledge, request, response, callback), latency.toNanos(), TimeUnit.NANOSECONDS);
            return true;
        };
    }

    /** Answers with Vespa's JSON error body for the requested path. */
    public static Request.Handler vespaError(int status, String message) {
        return (request, response, callback) -> {
            String body = "{\"pathId\":\"" + request.getHttpURI().getPath() + "\",\"message\":\"" + message + "\"}";
            return respond(status, JSON, body).handle(request, response, callback);
        };
    }

    /** Answers with a body that need not be Vespa JSON, as a proxy or load balancer in front of Vespa does. */
    public static Request.Handler respond(int status, String contentType, String body) {
        return (request, response, callback) -> {
            response.setStatus(status);
            if (contentType != null) {
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, contentType);
            }
            Content.Sink.write(response, true, body, callback);
            return true;
        };
    }

    /** Never answers, so the operation or handshake times out. */
    public static Request.Handler neverAnswer() {
        return (request, response, callback) -> true;
    }

    private static void answer(Request.Handler answer, Request request, Response response, Callback callback) {
        try {
            answer.handle(request, response, callback);
        } catch (Exception e) {
            callback.failed(e);
        }
    }

    private static MockVespaServer start(String scheme, Request.Handler handshake, Request.Handler operations,
                                         Function<Server, ServerConnector> connectors) throws Exception {
        Server server = new Server();
        ServerConnector connector = connectors.apply(server);
        connector.setHost("localhost");
        server.addConnector(connector);
        server.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) throws Exception {
                boolean isHandshake = request.getHttpURI().getPath().equals(HANDSHAKE_PATH);
                return (isHandshake ? handshake : operations).handle(request, response, callback);
            }
        });
        server.start();
        return new MockVespaServer(server, scheme);
    }

    public String url() {
        return scheme + "://localhost:" + ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    @Override
    public void close() throws Exception {
        server.stop();
    }
}
