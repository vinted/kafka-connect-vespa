package com.vinted.kafka.connect.vespa.factories;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedClientBuilder;
import ai.vespa.feed.client.FeedException;
import com.vinted.kafka.connect.vespa.VespaSinkConfig;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.io.EndPoint;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.HostnameVerifier;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VespaFeedClientFactoryTest {
    private static final String KEY_STORE_PASSWORD = "changeit";

    private final Map<String, String> params = new HashMap<>();
    private final List<Certificate> presentedClientCertificates = new CopyOnWriteArrayList<>();
    private Server httpsServer;
    private Identity server;
    private Identity client;

    @TempDir
    Path tempDir;

    @Test
    void configuresDefaults() {
        RecordingBuilder builder = configure();

        assertEquals(List.of(1), builder.args("setConnectionsPerEndpoint"));
        assertEquals(List.of(16), builder.args("setMaxStreamPerConnection"));
        assertEquals(List.of(Duration.ZERO), builder.args("setConnectionTimeToLive"));
        assertEquals(List.of(false), builder.args("setDryrun"));
        assertEquals(List.of(false), builder.args("setSpeedTest"));
        assertEquals(List.of(FeedClientBuilder.Compression.auto), builder.args("setCompression"));
        assertEquals(List.of(1), builder.args("setInitialInflightFactor"));
        assertTrue(builder.called("setCircuitBreaker"));
        assertTrue(builder.called("setRetryStrategy"));

        for (String method : Arrays.asList("noBenchmarking", "addRequestHeader", "setCertificate",
                "setCaCertificatesFile", "setHostnameVerifier", "setProxy", "setProxyCaCertificatesFile",
                "setProxyHostnameVerifier", "addProxyRequestHeader")) {
            assertFalse(builder.called(method), method);
        }
    }

    @Test
    void configuresClientOptions() {
        params.put("vespa.connection.ttl.ms", "60000");
        params.put("vespa.compression", "NONE");
        params.put("vespa.initial.inflight.factor", "8");
        params.put("vespa.benchmark", "false");
        params.put("vespa.request.headers", "X-Team:search");
        params.put("vespa.cloud.token", "token123");

        RecordingBuilder builder = configure();

        assertEquals(List.of(Duration.ofMinutes(1)), builder.args("setConnectionTimeToLive"));
        assertEquals(List.of(FeedClientBuilder.Compression.none), builder.args("setCompression"));
        assertEquals(List.of(8), builder.args("setInitialInflightFactor"));
        assertTrue(builder.called("noBenchmarking"));
        assertEquals(
                List.of("X-Team", "search", "Authorization", "Bearer token123"),
                builder.args("addRequestHeader"));
    }

    @Test
    void configuresMutualTls() {
        params.put("vespa.ssl.certificate.path", "/certs/client.pem");
        params.put("vespa.ssl.private.key.path", "/certs/client.key");
        params.put("vespa.ssl.ca.certificates.path", "/certs/ca.pem");
        params.put("vespa.ssl.hostname.verification.enabled", "false");

        RecordingBuilder builder = configure();

        assertEquals(
                List.of(Paths.get("/certs/client.pem"), Paths.get("/certs/client.key")),
                builder.args("setCertificate"));
        assertEquals(List.of(Paths.get("/certs/ca.pem")), builder.args("setCaCertificatesFile"));
        HostnameVerifier verifier = (HostnameVerifier) builder.args("setHostnameVerifier").get(0);
        assertTrue(verifier.verify("any.host", null));
    }

    @Test
    void configuresProxy() {
        params.put("vespa.proxy.url", "https://proxy:3128");
        params.put("vespa.proxy.ca.certificates.path", "/certs/proxy-ca.pem");
        params.put("vespa.proxy.hostname.verification.enabled", "false");
        params.put("vespa.proxy.request.headers", "Proxy-Authorization:Basic abc");

        RecordingBuilder builder = configure();

        assertEquals(List.of(URI.create("https://proxy:3128")), builder.args("setProxy"));
        assertEquals(List.of(Paths.get("/certs/proxy-ca.pem")), builder.args("setProxyCaCertificatesFile"));
        assertTrue(builder.called("setProxyHostnameVerifier"));
        assertEquals(List.of("Proxy-Authorization", "Basic abc"), builder.args("addProxyRequestHeader"));
    }

    @Test
    void connectsToServerRequiringClientCertificate() throws Exception {
        startMutualTlsServer();
        params.put("vespa.endpoint", serverUrl());
        params.put("vespa.ssl.certificate.path", writePem("client.pem", client.certificate).toString());
        params.put("vespa.ssl.private.key.path", writePem("client.key", client.keyPair.getPrivate()).toString());
        params.put("vespa.ssl.ca.certificates.path", writePem("ca.pem", server.certificate).toString());

        FeedClient feedClient = VespaFeedClientFactory.create(new VespaSinkConfig(params));
        feedClient.close();

        assertEquals(List.of(client.certificate), presentedClientCertificates);
    }

    @Test
    void isRejectedByServerRequiringClientCertificateWhenNoneConfigured() throws Exception {
        startMutualTlsServer();
        params.put("vespa.endpoint", serverUrl());
        params.put("vespa.ssl.ca.certificates.path", writePem("ca.pem", server.certificate).toString());

        assertThrows(FeedException.class, () -> VespaFeedClientFactory.create(new VespaSinkConfig(params)));
        assertTrue(presentedClientCertificates.isEmpty());
    }

    @Test
    void failsWhenCertificateFilesAreMissing() {
        params.put("vespa.endpoint", "https://localhost:8443");
        params.put("vespa.ssl.certificate.path", tempDir.resolve("missing.pem").toString());
        params.put("vespa.ssl.private.key.path", tempDir.resolve("missing.key").toString());

        assertThrows(UncheckedIOException.class, () -> VespaFeedClientFactory.create(new VespaSinkConfig(params)));
    }

    @AfterEach
    void stopServer() throws Exception {
        if (httpsServer != null) {
            httpsServer.stop();
        }
    }

    private RecordingBuilder configure() {
        RecordingBuilder recorder = new RecordingBuilder();
        VespaFeedClientFactory.configure(recorder.builder, new VespaSinkConfig(params));
        return recorder;
    }

    /** Starts an HTTPS server that only accepts clients presenting {@link #client}'s certificate. */
    private void startMutualTlsServer() throws Exception {
        server = Identity.create("localhost");
        client = Identity.create("kafka-connect-vespa");

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("server", server.keyPair.getPrivate(), KEY_STORE_PASSWORD.toCharArray(),
                new X509Certificate[]{server.certificate});

        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("client", client.certificate);

        SslContextFactory.Server tls = new SslContextFactory.Server();
        tls.setKeyStore(keyStore);
        tls.setKeyStorePassword(KEY_STORE_PASSWORD);
        tls.setTrustStore(trustStore);
        tls.setNeedClientAuth(true);

        HttpConfiguration httpConfiguration = new HttpConfiguration();
        httpConfiguration.addCustomizer(new SecureRequestCustomizer());
        HTTP2ServerConnectionFactory http2 = new HTTP2ServerConnectionFactory(httpConfiguration);
        ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
        alpn.setDefaultProtocol(http2.getProtocol());

        httpsServer = new Server();
        ServerConnector connector = new ServerConnector(
                httpsServer, new SslConnectionFactory(tls, alpn.getProtocol()), alpn, http2);
        connector.setHost("localhost");
        httpsServer.addConnector(connector);
        httpsServer.setHandler(new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) {
                EndPoint.SslSessionData session =
                        (EndPoint.SslSessionData) request.getAttribute(EndPoint.SslSessionData.ATTRIBUTE);
                presentedClientCertificates.addAll(Arrays.asList(session.peerCertificates()));
                response.setStatus(200);
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json");
                Content.Sink.write(response, true, "{}", callback);
                return true;
            }
        });
        httpsServer.start();
    }

    private String serverUrl() {
        return "https://localhost:" + ((ServerConnector) httpsServer.getConnectors()[0]).getLocalPort();
    }

    private Path writePem(String fileName, Object object) throws IOException {
        Path path = tempDir.resolve(fileName);
        try (Writer writer = Files.newBufferedWriter(path); JcaPEMWriter pem = new JcaPEMWriter(writer)) {
            pem.writeObject(object);
        }
        return path;
    }

    /** A self-signed certificate for {@code hostname} and its key pair. */
    private static final class Identity {
        final KeyPair keyPair;
        final X509Certificate certificate;

        private Identity(KeyPair keyPair, X509Certificate certificate) {
            this.keyPair = keyPair;
            this.certificate = certificate;
        }

        static Identity create(String hostname) throws Exception {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair keyPair = generator.generateKeyPair();

            X500Name subject = new X500Name("CN=" + hostname);
            Instant now = Instant.now();
            X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(
                    new JcaX509v3CertificateBuilder(
                            subject,
                            BigInteger.valueOf(now.toEpochMilli()),
                            Date.from(now.minus(Duration.ofDays(1))),
                            Date.from(now.plus(Duration.ofDays(1))),
                            subject,
                            keyPair.getPublic())
                            .addExtension(Extension.subjectAlternativeName, false,
                                    new GeneralNames(new GeneralName(GeneralName.dNSName, hostname)))
                            .build(new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate())));
            return new Identity(keyPair, certificate);
        }
    }

    /** Records every builder invocation; a dynamic proxy keeps it independent of the builder's method set. */
    private static class RecordingBuilder {
        private final List<Object[]> calls = new ArrayList<>();
        final FeedClientBuilder builder = (FeedClientBuilder) Proxy.newProxyInstance(
                FeedClientBuilder.class.getClassLoader(),
                new Class<?>[]{FeedClientBuilder.class},
                (proxy, method, args) -> {
                    Object[] call = new Object[(args == null ? 0 : args.length) + 1];
                    call[0] = method.getName();
                    if (args != null) {
                        System.arraycopy(args, 0, call, 1, args.length);
                    }
                    calls.add(call);
                    return method.getReturnType() == FeedClientBuilder.class ? proxy : null;
                });

        boolean called(String method) {
            return calls.stream().anyMatch(call -> call[0].equals(method));
        }

        List<Object> args(String method) {
            return calls.stream()
                    .filter(call -> call[0].equals(method))
                    .flatMap(call -> Arrays.stream(call).skip(1))
                    .collect(Collectors.toList());
        }
    }
}
