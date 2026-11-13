package com.vinted.kafka.connect.vespa.mocks;

import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.function.Function;

/** A local HTTP/2 server for the real feed client to talk to, answering every request with the given handler. */
public final class MockVespaServer implements AutoCloseable {
    private final Server server;
    private final String scheme;

    private MockVespaServer(Server server, String scheme) {
        this.server = server;
        this.scheme = scheme;
    }

    /** Serves cleartext HTTP/2, which the feed client speaks to an {@code http://} endpoint. */
    public static MockVespaServer http(Handler handler) throws Exception {
        return start("http", handler, server -> new ServerConnector(
                server, new HTTP2CServerConnectionFactory(new HttpConfiguration())));
    }

    public static MockVespaServer https(SslContextFactory.Server tls, Handler handler) throws Exception {
        HttpConfiguration configuration = new HttpConfiguration();
        configuration.addCustomizer(new SecureRequestCustomizer());
        HTTP2ServerConnectionFactory http2 = new HTTP2ServerConnectionFactory(configuration);
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol(http2.getProtocol());

        return start("https", handler, server -> new ServerConnector(
                server, new SslConnectionFactory(tls, alpn.getProtocol()), alpn, http2));
    }

    private static MockVespaServer start(
            String scheme, Handler handler, Function<Server, ServerConnector> connectors) throws Exception {
        Server server = new Server();
        ServerConnector connector = connectors.apply(server);
        connector.setHost("localhost");
        server.addConnector(connector);
        server.setHandler(handler);
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
