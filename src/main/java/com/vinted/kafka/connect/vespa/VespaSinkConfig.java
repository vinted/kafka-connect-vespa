package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.FeedClient;
import ai.vespa.feed.client.FeedClientBuilder;
import ai.vespa.feed.client.OperationParameters;
import ai.vespa.feed.client.impl.GracePeriodCircuitBreaker;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import com.google.common.base.Strings;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import javax.net.ssl.HostnameVerifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;

/** The connector's settings, and the feed client, operation parameters and policies they configure. */
public class VespaSinkConfig extends AbstractConfig {
    private static final HostnameVerifier NO_HOSTNAME_VERIFICATION = (hostname, session) -> true;

    // Connector group
    public static final String VESPA_CLOUD_TOKEN_CONFIG = "vespa.cloud.token";
    private static final String VESPA_CLOUD_TOKEN_DEFAULT = null;
    private static final String VESPA_CLOUD_TOKEN_DOC = "Vespa Cloud token when using token based authentication. "
            + "Sent as an `Authorization: Bearer` header and masked in logs.";
    private static final String VESPA_CLOUD_TOKEN_DISPLAY = "Vespa cloud token";

    public static final String ENDPOINT_CONFIG = "vespa.endpoint";
    private static final String ENDPOINT_DOC = "The comma-separated list of one or more Vespa URLs, such as "
            + "`https://node1:8080,http://node2:8080` or `https://node3:8080`. HTTPS is used for all connections "
            + "if any of the URLs starts with `https:`. A URL without a protocol is treated as `http`.";
    private static final String ENDPOINT_DISPLAY = "Endpoint URLs";
    private static final String ENDPOINT_DEFAULT = "http://localhost:8080";

    public static final String CONNECTIONS_PER_ENDPOINT_CONFIG = "vespa.connections.per.endpoint";
    private static final String CONNECTIONS_PER_ENDPOINT_DOC = "The number of connections the feed client uses to each "
            + "endpoint. A reasonable value lets all feed clients (if more than one) collectively have a number of "
            + "connections which is a small multiple of the number of containers in the cluster to feed, so load can "
            + "be balanced across these containers. In general, this value should be kept as low as possible, but "
            + "poor connectivity between feeder and cluster may also warrant a higher number of connections.";
    private static final String CONNECTIONS_PER_ENDPOINT_DISPLAY = "Connections per endpoint";

    private static final int CONNECTIONS_PER_ENDPOINT_DEFAULT = 1;

    public static final String MAX_STREAMS_PER_CONNECTION_CONFIG = "vespa.max.streams.per.connection";
    private static final String MAX_STREAMS_PER_CONNECTION_DOC = "The maximum number of streams per HTTP/2 connection "
            + "of the feed client. This determines the maximum number of concurrent, in-flight requests of the "
            + "client, which is `vespa.connections.per.endpoint` times this value per endpoint. Prefer more streams "
            + "to more connections, when possible. The feed client automatically throttles load to achieve the best "
            + "throughput, and the actual number of streams per connection is usually lower than the maximum.";
    private static final String MAX_STREAMS_PER_CONNECTION_DISPLAY = "Max streams per connection";
    private static final int MAX_STREAMS_PER_CONNECTION_DEFAULT = 16;

    public static final String DRYRUN_CONFIG = "vespa.dryrun";
    private static final String DRYRUN_DOC = "Turns on dryrun mode, where each operation succeeds after a given "
            + "delay, rather than being sent across the network.";
    private static final String DRYRUN_DISPLAY = "Dryrun";
    private static final boolean DRYRUN_DEFAULT = false;

    public static final String SPEED_TEST_CONFIG = "vespa.speedtest";
    private static final String SPEED_TEST_DOC = "Turns on speed test mode, where each operation succeeds "
            + "immediately, rather than being sent across the network.";
    private static final String SPEED_TEST_DISPLAY = "Speed test";
    private static final boolean SPEED_TEST_DEFAULT = false;

    public static final String MAX_FAILURE_MS_CONFIG = "vespa.max.failure.ms";
    private static final String MAX_FAILURE_MS_DOC = "The period of consecutive failures after which the "
            + "feed client's circuit breaker opens; the connector then builds a new feed client.";
    private static final String MAX_FAILURE_MS_DISPLAY = "Max failure ms";
    private static final int MAX_FAILURE_MS_DEFAULT = 60000;

    public static final String NAMESPACE_CONFIG = "vespa.namespace";
    private static final String NAMESPACE_DOC = "User specified part of each document ID in that sense. Namespace can "
            + "not be used in queries, other than as part of the full document ID. However, it can be used for "
            + "document selection, where namespace can be accessed and compared to a given string, for instance. "
            + "An example use case is visiting a subset of documents. Defaults to topic name if not specified.";
    private static final String NAMESPACE_DISPLAY = "Namespace";
    private static final String NAMESPACE_DEFAULT = null;

    public static final String DOCUMENT_TYPE_CONFIG = "vespa.document.type";
    private static final String DOCUMENT_TYPE_DOC = "Document type as defined in services.xml and the schema. "
            + "Defaults to topic name if not specified.";
    private static final String DOCUMENT_TYPE_DISPLAY = "Document type";
    private static final String DOCUMENT_TYPE_DEFAULT = null;

    public static final String OPERATIONAL_MODE_CONFIG = "vespa.operational.mode";
    private static final String OPERATIONAL_MODE_DOC = "The operational mode of the connector. Valid options are "
            + "upsert and raw. Upsert mode puts each record as a whole document, replacing any existing one, and "
            + "turns tombstones into removes. Raw mode executes all operations using document "
            + "json format as explained in https://docs.vespa.ai/en/reference/document-json-format.html.";
    private static final String OPERATIONAL_MODE_DISPLAY = "Operational mode";
    private static final OperationalMode OPERATIONAL_MODE_DEFAULT = OperationalMode.UPSERT;

    public static final String RETRY_STRATEGY_OPERATION_TYPES_CONFIG = "vespa.retry.strategy.operation.types";
    private static final String RETRY_STRATEGY_OPERATION_TYPES_DOC = "Operation types to retry.";
    private static final String RETRY_STRATEGY_OPERATION_TYPES_DISPLAY = "Operation types to retry.";
    private static final List<FeedClient.OperationType> RETRY_STRATEGY_OPERATION_TYPES_DEFAULT = Arrays.asList(FeedClient.OperationType.values());

    public static final String RETRY_STRATEGY_RETRIES_CONFIG = "vespa.retry.strategy.retries";
    private static final String RETRY_STRATEGY_RETRIES_DOC = "Number of retries per operation for assumed transient, "
            + "non-backpressure problems.";
    private static final String RETRY_STRATEGY_RETRIES_DISPLAY = "Max Retries";
    private static final int RETRY_STRATEGY_RETRIES_DEFAULT = 10;

    public static final String MAX_RETRIES_CONFIG = "vespa.max.retries";
    private static final String MAX_RETRIES_DOC = "How many times in a row to retry a batch that failed only because "
            + "Vespa or the network was unavailable, such as during a Vespa upgrade: network errors, timeouts, server "
            + "errors (5xx) and responses that are not Vespa JSON. Client errors (4xx) are not retried, except "
            + "a 4xx whose body is not Vespa JSON, as from a proxy, since the feed client loses its status. Between "
            + "retries the task pauses its partitions for a random backoff between 0 and "
            + "`vespa.retry.backoff.ms * 2^retry`, also capped by the time left until the next offset commit, then "
            + "redelivers the whole batch. The count resets after every successful batch; once it is exhausted the "
            + "task fails. These retries come on top of the feed client's own immediate retries, "
            + "`vespa.retry.strategy.retries`.";
    private static final String MAX_RETRIES_DISPLAY = "Max retries";
    private static final int MAX_RETRIES_DEFAULT = 10;

    public static final String RETRY_BACKOFF_MS_CONFIG = "vespa.retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "Initial backoff before retrying a batch after Vespa or the "
            + "network was unavailable. Each retry waits a random time between 0 and twice the previous upper bound. "
            + "See `vespa.max.retries`.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry backoff ms";
    private static final long RETRY_BACKOFF_MS_DEFAULT = 1000L;

    public static final String CIRCUIT_BREAKER_GRACE_PERIOD_MS_CONFIG = "vespa.circuit.breaker.grace.period.ms";
    private static final String CIRCUIT_BREAKER_GRACE_PERIOD_MS_DOC = "The period of consecutive failures before the "
            + "circuit breaker changes state to half-open, during which the feed client sends only a single request "
            + "at a time to probe the cluster. See `vespa.max.failure.ms` for the period after which the "
            + "client shuts down entirely.";
    private static final String CIRCUIT_BREAKER_GRACE_PERIOD_MS_DISPLAY = "Circuit breaker grace period ms";
    private static final long CIRCUIT_BREAKER_GRACE_PERIOD_MS_DEFAULT = 10000L;

    public static final String CONNECTION_TTL_MS_CONFIG = "vespa.connection.ttl.ms";
    private static final String CONNECTION_TTL_MS_DOC = "Duration after which the feed client recycles active "
            + "connections. Useful for rebalancing connections behind a load balancer. `0` disables recycling.";
    private static final String CONNECTION_TTL_MS_DISPLAY = "Connection TTL ms";
    private static final long CONNECTION_TTL_MS_DEFAULT = 0L;

    public static final String COMPRESSION_CONFIG = "vespa.compression";
    private static final String COMPRESSION_DOC = "Compression to use for request bodies. `AUTO` compresses large "
            + "request bodies only.";
    private static final String COMPRESSION_DISPLAY = "Compression";
    private static final FeedClientBuilder.Compression COMPRESSION_DEFAULT = FeedClientBuilder.Compression.auto;

    public static final String INITIAL_INFLIGHT_FACTOR_CONFIG = "vespa.initial.inflight.factor";
    private static final String INITIAL_INFLIGHT_FACTOR_DOC = "Initial inflight factor of the feed client's "
            + "throttler. The initial target of inflight operations is `minInflight * initialInflightFactor`; the "
            + "throttler then adjusts it based on observed throughput.";
    private static final String INITIAL_INFLIGHT_FACTOR_DISPLAY = "Initial inflight factor";
    private static final int INITIAL_INFLIGHT_FACTOR_DEFAULT = 1;

    public static final String BENCHMARK_CONFIG = "vespa.benchmark";
    private static final String BENCHMARK_DOC = "Whether the feed client collects operation statistics. The "
            + "connector does not read them, so disabling this saves a little overhead.";
    private static final String BENCHMARK_DISPLAY = "Benchmark";
    private static final boolean BENCHMARK_DEFAULT = true;

    public static final String REQUEST_HEADERS_CONFIG = "vespa.request.headers";
    private static final String REQUEST_HEADERS_DOC = "Additional HTTP headers sent with every request to Vespa, as "
            + "a comma-separated list of `Name:Value` pairs, such as `X-Team:search,X-Env:prod`. Header values "
            + "cannot contain commas. Values are logged in plain text, so use `vespa.cloud.token` for the Vespa "
            + "Cloud token.";
    private static final String REQUEST_HEADERS_DISPLAY = "Request headers";

    // Security group
    public static final String SSL_CERTIFICATE_PATH_CONFIG = "vespa.ssl.certificate.path";
    private static final String SSL_CERTIFICATE_PATH_DOC = "Path to a PEM file with the client certificate used for "
            + "mutual TLS, such as the data plane certificate of a Vespa Cloud application. Must be set together "
            + "with `vespa.ssl.private.key.path`.";
    private static final String SSL_CERTIFICATE_PATH_DISPLAY = "Client certificate path";

    public static final String SSL_PRIVATE_KEY_PATH_CONFIG = "vespa.ssl.private.key.path";
    private static final String SSL_PRIVATE_KEY_PATH_DOC = "Path to a PEM file with the private key of the client "
            + "certificate. Must be set together with `vespa.ssl.certificate.path`.";
    private static final String SSL_PRIVATE_KEY_PATH_DISPLAY = "Client private key path";

    public static final String SSL_CA_CERTIFICATES_PATH_CONFIG = "vespa.ssl.ca.certificates.path";
    private static final String SSL_CA_CERTIFICATES_PATH_DOC = "Path to a PEM file with trusted CA certificates. "
            + "Overrides the JVM default truststore.";
    private static final String SSL_CA_CERTIFICATES_PATH_DISPLAY = "CA certificates path";

    public static final String SSL_HOSTNAME_VERIFICATION_ENABLED_CONFIG = "vespa.ssl.hostname.verification.enabled";
    private static final String SSL_HOSTNAME_VERIFICATION_ENABLED_DOC = "Whether to verify that the Vespa endpoint "
            + "hostname matches its TLS certificate. Disable only for testing.";
    private static final String SSL_HOSTNAME_VERIFICATION_ENABLED_DISPLAY = "Hostname verification";
    private static final boolean SSL_HOSTNAME_VERIFICATION_ENABLED_DEFAULT = true;

    // Proxy group
    public static final String PROXY_URL_CONFIG = "vespa.proxy.url";
    private static final String PROXY_URL_DOC = "HTTP or HTTPS proxy URL used for all endpoints, such as "
            + "`http://proxy:3128`.";
    private static final String PROXY_URL_DISPLAY = "Proxy URL";

    public static final String PROXY_CA_CERTIFICATES_PATH_CONFIG = "vespa.proxy.ca.certificates.path";
    private static final String PROXY_CA_CERTIFICATES_PATH_DOC = "Path to a PEM file with trusted CA certificates "
            + "for an HTTPS proxy. Overrides the JVM default truststore.";
    private static final String PROXY_CA_CERTIFICATES_PATH_DISPLAY = "Proxy CA certificates path";

    public static final String PROXY_HOSTNAME_VERIFICATION_ENABLED_CONFIG = "vespa.proxy.hostname.verification.enabled";
    private static final String PROXY_HOSTNAME_VERIFICATION_ENABLED_DOC = "Whether to verify that the HTTPS proxy "
            + "hostname matches its TLS certificate. Disable only for testing.";
    private static final String PROXY_HOSTNAME_VERIFICATION_ENABLED_DISPLAY = "Proxy hostname verification";
    private static final boolean PROXY_HOSTNAME_VERIFICATION_ENABLED_DEFAULT = true;

    public static final String PROXY_REQUEST_HEADERS_CONFIG = "vespa.proxy.request.headers";
    private static final String PROXY_REQUEST_HEADERS_DOC = "Additional HTTP headers sent with every request to the "
            + "proxy, as a comma-separated list of `Name:Value` pairs, such as `Proxy-Authorization:Basic abc`. "
            + "Header values cannot contain commas and are logged in plain text.";
    private static final String PROXY_REQUEST_HEADERS_DISPLAY = "Proxy request headers";

    // Operation group
    public static final String OPERATION_TIMEOUT_MS_CONFIG = "vespa.operation.timeout.ms";
    private static final String OPERATION_TIMEOUT_MS_DOC = "Feed operation timeout.";
    private static final String OPERATION_TIMEOUT_MS_DISPLAY = "Operation timeout";
    private static final int OPERATION_TIMEOUT_MS_DEFAULT = 60000;

    public static final String OPERATION_ROUTE_CONFIG = "vespa.operation.route";
    private static final String OPERATION_ROUTE_DOC = "Target Vespa route for feed operations.";
    private static final String OPERATION_ROUTE_DISPLAY = "Operation route";
    private static final String OPERATION_ROUTE_DEFAULT = null;

    public static final String OPERATION_TRACELEVEL_CONFIG = "vespa.operation.tracelevel";
    private static final String OPERATION_TRACELEVEL_DOC = "The trace level of network traffic.";
    private static final String OPERATION_TRACELEVEL_DISPLAY = "Operation tracelevel";
    private static final int OPERATION_TRACELEVEL_DEFAULT = 0;

    public static final String OPERATION_CONDITION_CONFIG = "vespa.operation.condition";
    private static final String OPERATION_CONDITION_DOC = "Test-and-set condition applied to every put and remove "
            + "in UPSERT mode, such as `music.locked == false`. Operations whose condition is not met are handled "
            + "according to `vespa.behavior.on.malformed.documents`. Ignored in RAW mode, where each "
            + "operation carries its own `condition`.";
    private static final String OPERATION_CONDITION_DISPLAY = "Operation condition";

    public static final String OPERATION_CREATE_IF_NON_EXISTENT_CONFIG = "vespa.operation.create.if.non.existent";
    private static final String OPERATION_CREATE_IF_NON_EXISTENT_DOC = "In UPSERT mode, create the document when a "
            + "conditional put targets a document that does not exist, instead of failing the condition. Only "
            + "meaningful together with `vespa.operation.condition`. Ignored in RAW mode, where each "
            + "operation carries its own `create`.";
    private static final String OPERATION_CREATE_IF_NON_EXISTENT_DISPLAY = "Create if non-existent";
    private static final boolean OPERATION_CREATE_IF_NON_EXISTENT_DEFAULT = false;

    // Data conversion group

    public static final String DROP_INVALID_MESSAGE_CONFIG = "vespa.drop.invalid.message";
    private static final String DROP_INVALID_MESSAGE_DOC = "Whether to drop kafka message when it cannot be "
            + "converted to output message.";
    private static final String DROP_INVALID_MESSAGE_DISPLAY = "Drop invalid messages";
    private static final boolean DROP_INVALID_MESSAGE_DEFAULT = false;

    public static final String BEHAVIOR_ON_MALFORMED_DOCS_CONFIG = "vespa.behavior.on.malformed.documents";
    private static final String BEHAVIOR_ON_MALFORMED_DOCS_DOC = "How to handle malformed documents (an HTTP "
            + "400, or document JSON the feed client cannot parse) and operations whose test-and-set condition is not "
            + "met. Valid options are `IGNORE`, `WARN`, and `FAIL`.";
    private static final String BEHAVIOR_ON_MALFORMED_DOCS_DISPLAY = "Behavior on malformed documents";
    private static final BehaviorOnMalformedDoc BEHAVIOR_ON_MALFORMED_DOCS_DEFAULT = BehaviorOnMalformedDoc.FAIL;

    private static final String CONNECTOR_GROUP = "Connector";
    private static final String OPERATION_GROUP = "Operation";
    private static final String DATA_CONVERSION_GROUP = "Data Conversion";
    private static final String SECURITY_GROUP = "Security";
    private static final String PROXY_GROUP = "Proxy";

    private static final ConfigDef.Validator NON_EMPTY_STRING =
            ConfigDef.NonEmptyStringWithoutControlChars.nonEmptyStringWithoutControlChars();

    private static final ConfigDef.Validator NON_EMPTY_PASSWORD = ConfigDef.LambdaValidator.with(
            (name, value) -> {
                if (value != null && ((Password) value).value().isEmpty()) {
                    throw new ConfigException(name, "[hidden]", "Value must not be empty");
                }
            },
            () -> "non-empty string");

    private static final ConfigDef.Validator HEADERS = ConfigDef.LambdaValidator.with(
            (name, value) -> {
                try {
                    parseHeaders(name, (List<?>) value);
                } catch (IllegalArgumentException e) {
                    throw new ConfigException(name, value, e.getMessage());
                }
            },
            () -> "comma-separated list of `Name:Value` pairs");

    private static final ConfigDef.Validator OPTIONAL_URL = ConfigDef.LambdaValidator.with(
            (name, value) -> {
                if (value == null) {
                    return;
                }
                try {
                    URI uri = new URI((String) value);
                    if (!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme()) || uri.getHost() == null) {
                        throw new ConfigException(name, value, "Must be an http or https URL with a host");
                    }
                } catch (URISyntaxException e) {
                    throw new ConfigException(name, value, e.getMessage());
                }
            },
            () -> "http or https URL");

    public static final ConfigDef CONFIG = baseConfigDef();

    protected static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectorConfigs(configDef);
        addOperationConfigs(configDef);
        addDataConversionConfigs(configDef);
        addSecurityConfigs(configDef);
        addProxyConfigs(configDef);
        return configDef;
    }

    private static void addConnectorConfigs(ConfigDef configDef) {
        int order = 0;
        configDef
                .define(
                        ENDPOINT_CONFIG,
                        ConfigDef.Type.LIST,
                        ENDPOINT_DEFAULT,
                        Validators.validUrl(),
                        ConfigDef.Importance.HIGH,
                        ENDPOINT_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        ENDPOINT_DISPLAY)
                .define(
                        VESPA_CLOUD_TOKEN_CONFIG,
                        ConfigDef.Type.PASSWORD,
                        VESPA_CLOUD_TOKEN_DEFAULT,
                        NON_EMPTY_PASSWORD,
                        ConfigDef.Importance.LOW,
                        VESPA_CLOUD_TOKEN_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        VESPA_CLOUD_TOKEN_DISPLAY)
                .define(
                        CONNECTIONS_PER_ENDPOINT_CONFIG,
                        ConfigDef.Type.INT,
                        CONNECTIONS_PER_ENDPOINT_DEFAULT,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        CONNECTIONS_PER_ENDPOINT_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        CONNECTIONS_PER_ENDPOINT_DISPLAY)
                .define(
                        MAX_STREAMS_PER_CONNECTION_CONFIG,
                        ConfigDef.Type.INT,
                        MAX_STREAMS_PER_CONNECTION_DEFAULT,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        MAX_STREAMS_PER_CONNECTION_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        MAX_STREAMS_PER_CONNECTION_DISPLAY)
                .define(
                        DRYRUN_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DRYRUN_DEFAULT,
                        ConfigDef.Importance.LOW,
                        DRYRUN_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        DRYRUN_DISPLAY)
                .define(
                        SPEED_TEST_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        SPEED_TEST_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SPEED_TEST_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        SPEED_TEST_DISPLAY)
                .define(
                        MAX_FAILURE_MS_CONFIG,
                        ConfigDef.Type.INT,
                        MAX_FAILURE_MS_DEFAULT,
                        atLeast(10000),
                        ConfigDef.Importance.LOW,
                        MAX_FAILURE_MS_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        MAX_FAILURE_MS_DISPLAY)
                .define(
                        NAMESPACE_CONFIG,
                        ConfigDef.Type.STRING,
                        NAMESPACE_DEFAULT,
                        ConfigDef.NonEmptyStringWithoutControlChars.nonEmptyStringWithoutControlChars(),
                        ConfigDef.Importance.HIGH,
                        NAMESPACE_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        NAMESPACE_DISPLAY)
                .define(
                        DOCUMENT_TYPE_CONFIG,
                        ConfigDef.Type.STRING,
                        DOCUMENT_TYPE_DEFAULT,
                        ConfigDef.NonEmptyStringWithoutControlChars.nonEmptyStringWithoutControlChars(),
                        ConfigDef.Importance.HIGH,
                        DOCUMENT_TYPE_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        DOCUMENT_TYPE_DISPLAY)
                .define(
                        OPERATIONAL_MODE_CONFIG,
                        ConfigDef.Type.STRING,
                        OPERATIONAL_MODE_DEFAULT.name(),
                        Validators.validEnum(OperationalMode.class),
                        ConfigDef.Importance.HIGH,
                        OPERATIONAL_MODE_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        OPERATIONAL_MODE_DISPLAY)
                .define(
                        RETRY_STRATEGY_RETRIES_CONFIG,
                        ConfigDef.Type.INT,
                        RETRY_STRATEGY_RETRIES_DEFAULT,
                        between(0, Integer.MAX_VALUE),
                        ConfigDef.Importance.LOW,
                        RETRY_STRATEGY_RETRIES_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        RETRY_STRATEGY_RETRIES_DISPLAY)
                .define(
                        RETRY_STRATEGY_OPERATION_TYPES_CONFIG,
                        ConfigDef.Type.LIST,
                        RETRY_STRATEGY_OPERATION_TYPES_DEFAULT.stream().map(Enum::name).collect(Collectors.joining(",")),
                        Validators.validEnum(FeedClient.OperationType.class),
                        ConfigDef.Importance.LOW,
                        RETRY_STRATEGY_OPERATION_TYPES_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        RETRY_STRATEGY_OPERATION_TYPES_DISPLAY)
                .define(
                        MAX_RETRIES_CONFIG,
                        ConfigDef.Type.INT,
                        MAX_RETRIES_DEFAULT,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        MAX_RETRIES_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        MAX_RETRIES_DISPLAY)
                .define(
                        RETRY_BACKOFF_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        RETRY_BACKOFF_MS_DEFAULT,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        RETRY_BACKOFF_MS_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        RETRY_BACKOFF_MS_DISPLAY)
                .define(
                        CIRCUIT_BREAKER_GRACE_PERIOD_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CIRCUIT_BREAKER_GRACE_PERIOD_MS_DEFAULT,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CIRCUIT_BREAKER_GRACE_PERIOD_MS_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        CIRCUIT_BREAKER_GRACE_PERIOD_MS_DISPLAY)
                .define(
                        CONNECTION_TTL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        CONNECTION_TTL_MS_DEFAULT,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CONNECTION_TTL_MS_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        CONNECTION_TTL_MS_DISPLAY)
                .define(
                        COMPRESSION_CONFIG,
                        ConfigDef.Type.STRING,
                        COMPRESSION_DEFAULT.name().toUpperCase(Locale.ROOT),
                        ConfigDef.CaseInsensitiveValidString.in(Arrays.stream(FeedClientBuilder.Compression.values())
                                .map(c -> c.name().toUpperCase(Locale.ROOT))
                                .toArray(String[]::new)),
                        ConfigDef.Importance.LOW,
                        COMPRESSION_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        COMPRESSION_DISPLAY)
                .define(
                        INITIAL_INFLIGHT_FACTOR_CONFIG,
                        ConfigDef.Type.INT,
                        INITIAL_INFLIGHT_FACTOR_DEFAULT,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        INITIAL_INFLIGHT_FACTOR_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        INITIAL_INFLIGHT_FACTOR_DISPLAY)
                .define(
                        BENCHMARK_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        BENCHMARK_DEFAULT,
                        ConfigDef.Importance.LOW,
                        BENCHMARK_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        BENCHMARK_DISPLAY)
                .define(
                        REQUEST_HEADERS_CONFIG,
                        ConfigDef.Type.LIST,
                        "",
                        HEADERS,
                        ConfigDef.Importance.LOW,
                        REQUEST_HEADERS_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        REQUEST_HEADERS_DISPLAY);
    }

    private static void addSecurityConfigs(ConfigDef configDef) {
        int order = 0;
        configDef
                .define(
                        SSL_CERTIFICATE_PATH_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        NON_EMPTY_STRING,
                        ConfigDef.Importance.MEDIUM,
                        SSL_CERTIFICATE_PATH_DOC,
                        SECURITY_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        SSL_CERTIFICATE_PATH_DISPLAY)
                .define(
                        SSL_PRIVATE_KEY_PATH_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        NON_EMPTY_STRING,
                        ConfigDef.Importance.MEDIUM,
                        SSL_PRIVATE_KEY_PATH_DOC,
                        SECURITY_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        SSL_PRIVATE_KEY_PATH_DISPLAY)
                .define(
                        SSL_CA_CERTIFICATES_PATH_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        NON_EMPTY_STRING,
                        ConfigDef.Importance.LOW,
                        SSL_CA_CERTIFICATES_PATH_DOC,
                        SECURITY_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        SSL_CA_CERTIFICATES_PATH_DISPLAY)
                .define(
                        SSL_HOSTNAME_VERIFICATION_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        SSL_HOSTNAME_VERIFICATION_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        SSL_HOSTNAME_VERIFICATION_ENABLED_DOC,
                        SECURITY_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        SSL_HOSTNAME_VERIFICATION_ENABLED_DISPLAY);
    }

    private static void addProxyConfigs(ConfigDef configDef) {
        int order = 0;
        configDef
                .define(
                        PROXY_URL_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        OPTIONAL_URL,
                        ConfigDef.Importance.LOW,
                        PROXY_URL_DOC,
                        PROXY_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        PROXY_URL_DISPLAY)
                .define(
                        PROXY_CA_CERTIFICATES_PATH_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        NON_EMPTY_STRING,
                        ConfigDef.Importance.LOW,
                        PROXY_CA_CERTIFICATES_PATH_DOC,
                        PROXY_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        PROXY_CA_CERTIFICATES_PATH_DISPLAY)
                .define(
                        PROXY_HOSTNAME_VERIFICATION_ENABLED_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        PROXY_HOSTNAME_VERIFICATION_ENABLED_DEFAULT,
                        ConfigDef.Importance.LOW,
                        PROXY_HOSTNAME_VERIFICATION_ENABLED_DOC,
                        PROXY_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        PROXY_HOSTNAME_VERIFICATION_ENABLED_DISPLAY)
                .define(
                        PROXY_REQUEST_HEADERS_CONFIG,
                        ConfigDef.Type.LIST,
                        "",
                        HEADERS,
                        ConfigDef.Importance.LOW,
                        PROXY_REQUEST_HEADERS_DOC,
                        PROXY_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        PROXY_REQUEST_HEADERS_DISPLAY);
    }

    private static void addOperationConfigs(ConfigDef configDef) {
        int order = 0;
        configDef
                .define(
                        OPERATION_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        OPERATION_TIMEOUT_MS_DEFAULT,
                        between(0, Integer.MAX_VALUE),
                        ConfigDef.Importance.LOW,
                        OPERATION_TIMEOUT_MS_DOC,
                        OPERATION_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        OPERATION_TIMEOUT_MS_DISPLAY)
                .define(
                        OPERATION_ROUTE_CONFIG,
                        ConfigDef.Type.STRING,
                        OPERATION_ROUTE_DEFAULT,
                        ConfigDef.NonEmptyStringWithoutControlChars.nonEmptyStringWithoutControlChars(),
                        ConfigDef.Importance.LOW,
                        OPERATION_ROUTE_DOC,
                        OPERATION_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        OPERATION_ROUTE_DISPLAY)
                .define(
                        OPERATION_TRACELEVEL_CONFIG,
                        ConfigDef.Type.INT,
                        OPERATION_TRACELEVEL_DEFAULT,
                        between(0, 9),
                        ConfigDef.Importance.LOW,
                        OPERATION_TRACELEVEL_DOC,
                        OPERATION_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        OPERATION_TRACELEVEL_DISPLAY)
                .define(
                        OPERATION_CONDITION_CONFIG,
                        ConfigDef.Type.STRING,
                        null,
                        NON_EMPTY_STRING,
                        ConfigDef.Importance.LOW,
                        OPERATION_CONDITION_DOC,
                        OPERATION_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        OPERATION_CONDITION_DISPLAY)
                .define(
                        OPERATION_CREATE_IF_NON_EXISTENT_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        OPERATION_CREATE_IF_NON_EXISTENT_DEFAULT,
                        ConfigDef.Importance.LOW,
                        OPERATION_CREATE_IF_NON_EXISTENT_DOC,
                        OPERATION_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        OPERATION_CREATE_IF_NON_EXISTENT_DISPLAY);
    }

    private static void addDataConversionConfigs(ConfigDef configDef) {
        int order = 0;
        configDef
                .define(
                        DROP_INVALID_MESSAGE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        DROP_INVALID_MESSAGE_DEFAULT,
                        ConfigDef.Importance.LOW,
                        DROP_INVALID_MESSAGE_DOC,
                        DATA_CONVERSION_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        DROP_INVALID_MESSAGE_DISPLAY)
                .define(
                        BEHAVIOR_ON_MALFORMED_DOCS_CONFIG,
                        ConfigDef.Type.STRING,
                        BEHAVIOR_ON_MALFORMED_DOCS_DEFAULT.name(),
                        Validators.validEnum(BehaviorOnMalformedDoc.class),
                        ConfigDef.Importance.LOW,
                        BEHAVIOR_ON_MALFORMED_DOCS_DOC,
                        DATA_CONVERSION_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        BEHAVIOR_ON_MALFORMED_DOCS_DISPLAY);
    }

    public VespaSinkConfig(Map<String, ?> configProviderProps) {
        super(CONFIG, configProviderProps);

        if (path(SSL_CERTIFICATE_PATH_CONFIG).isPresent() != path(SSL_PRIVATE_KEY_PATH_CONFIG).isPresent()) {
            throw new ConfigException(String.format("'%s' and '%s' must be set together.",
                    SSL_CERTIFICATE_PATH_CONFIG, SSL_PRIVATE_KEY_PATH_CONFIG));
        }
    }

    /** Builds a feed client for the configured endpoints with every feed client option applied. */
    public FeedClient feedClient() {
        return configure(FeedClientBuilder.create(endpoints())).build();
    }

    FeedClientBuilder configure(FeedClientBuilder builder) {
        builder.setConnectionsPerEndpoint(getInt(CONNECTIONS_PER_ENDPOINT_CONFIG));
        builder.setMaxStreamPerConnection(getInt(MAX_STREAMS_PER_CONNECTION_CONFIG));
        builder.setConnectionTimeToLive(Duration.ofMillis(getLong(CONNECTION_TTL_MS_CONFIG)));
        builder.setDryrun(getBoolean(DRYRUN_CONFIG));
        builder.setSpeedTest(getBoolean(SPEED_TEST_CONFIG));
        builder.setCompression(FeedClientBuilder.Compression.valueOf(getString(COMPRESSION_CONFIG).toLowerCase(Locale.ROOT)));
        builder.setInitialInflightFactor(getInt(INITIAL_INFLIGHT_FACTOR_CONFIG));
        builder.setCircuitBreaker(new GracePeriodCircuitBreaker(
                Duration.ofMillis(getLong(CIRCUIT_BREAKER_GRACE_PERIOD_MS_CONFIG)),
                Duration.ofMillis(getInt(MAX_FAILURE_MS_CONFIG))));
        builder.setRetryStrategy(retryStrategy());

        if (!getBoolean(BENCHMARK_CONFIG)) {
            builder.noBenchmarking();
        }

        headers(REQUEST_HEADERS_CONFIG).forEach(builder::addRequestHeader);
        Optional.ofNullable(getPassword(VESPA_CLOUD_TOKEN_CONFIG))
                .ifPresent(token -> builder.addRequestHeader("Authorization", "Bearer " + token.value()));
        path(SSL_CERTIFICATE_PATH_CONFIG).ifPresent(certificate ->
                builder.setCertificate(certificate, path(SSL_PRIVATE_KEY_PATH_CONFIG).orElseThrow()));
        path(SSL_CA_CERTIFICATES_PATH_CONFIG).ifPresent(builder::setCaCertificatesFile);

        if (!getBoolean(SSL_HOSTNAME_VERIFICATION_ENABLED_CONFIG)) {
            builder.setHostnameVerifier(NO_HOSTNAME_VERIFICATION);
        }

        Optional.ofNullable(getString(PROXY_URL_CONFIG)).map(URI::create).ifPresent(builder::setProxy);
        path(PROXY_CA_CERTIFICATES_PATH_CONFIG).ifPresent(builder::setProxyCaCertificatesFile);

        if (!getBoolean(PROXY_HOSTNAME_VERIFICATION_ENABLED_CONFIG)) {
            builder.setProxyHostnameVerifier(NO_HOSTNAME_VERIFICATION);
        }

        headers(PROXY_REQUEST_HEADERS_CONFIG).forEach(builder::addProxyRequestHeader);

        return builder;
    }

    public OperationalMode operationalMode() {
        return ConfigUtils.getEnum(OperationalMode.class, this, OPERATIONAL_MODE_CONFIG);
    }

    /** The configured namespace, or the topic when none is. */
    public String namespace(String topic) {
        return Optional.ofNullable(getString(NAMESPACE_CONFIG)).orElse(topic);
    }

    /** The configured document type, or the topic when none is. */
    public String documentType(String topic) {
        return Optional.ofNullable(getString(DOCUMENT_TYPE_CONFIG)).orElse(topic);
    }

    /** The timeout, route and trace level, which every operational mode honours. */
    public OperationParameters operationParameters() {
        OperationParameters parameters = OperationParameters.empty();
        Duration timeout = Duration.ofMillis(getInt(OPERATION_TIMEOUT_MS_CONFIG));
        String route = getString(OPERATION_ROUTE_CONFIG);
        int tracelevel = getInt(OPERATION_TRACELEVEL_CONFIG);

        if (timeout.toMillis() > 0) {
            parameters = parameters.timeout(timeout);
        }

        if (!Strings.isNullOrEmpty(route)) {
            parameters = parameters.route(route);
        }

        if (tracelevel > 0) {
            parameters = parameters.tracelevel(tracelevel);
        }

        return parameters;
    }

    /** The operation parameters of an UPSERT remove: those of every mode plus the condition. */
    public OperationParameters removeParameters() {
        String condition = getString(OPERATION_CONDITION_CONFIG);
        OperationParameters parameters = operationParameters();
        return Strings.isNullOrEmpty(condition) ? parameters : parameters.testAndSetCondition(condition);
    }

    /** The operation parameters of an UPSERT put: those of a remove plus create if non-existent. */
    public OperationParameters putParameters() {
        return removeParameters().createIfNonExistent(getBoolean(OPERATION_CREATE_IF_NON_EXISTENT_CONFIG));
    }

    RetryBudget retryBudget(LongConsumer backOff) {
        return new RetryBudget(getInt(MAX_RETRIES_CONFIG), getLong(RETRY_BACKOFF_MS_CONFIG), backOff);
    }

    public boolean dropInvalidMessage() {
        return getBoolean(DROP_INVALID_MESSAGE_CONFIG);
    }

    public BehaviorOnMalformedDoc behaviorOnMalformedDoc() {
        return ConfigUtils.getEnum(BehaviorOnMalformedDoc.class, this, BEHAVIOR_ON_MALFORMED_DOCS_CONFIG);
    }

    private List<URI> endpoints() {
        Set<URI> endpoints = getList(ENDPOINT_CONFIG)
                .stream()
                .map(s -> s.endsWith("/") ? s : s + "/")
                .map(URI::create)
                .collect(Collectors.toCollection(HashSet::new));
        return List.copyOf(endpoints);
    }

    private FeedClient.RetryStrategy retryStrategy() {
        List<FeedClient.OperationType> types =
                ConfigUtils.getEnums(FeedClient.OperationType.class, this, RETRY_STRATEGY_OPERATION_TYPES_CONFIG);
        int retries = getInt(RETRY_STRATEGY_RETRIES_CONFIG);

        return new FeedClient.RetryStrategy() {
            @Override
            public boolean retry(FeedClient.OperationType type) {
                return types.contains(type);
            }

            @Override
            public int retries() {
                return retries;
            }
        };
    }

    private Map<String, String> headers(String key) {
        return parseHeaders(key, getList(key));
    }

    private Optional<Path> path(String key) {
        return Optional.ofNullable(getString(key)).map(Paths::get);
    }

    private static Map<String, String> parseHeaders(String name, List<?> entries) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (entries == null) {
            return headers;
        }
        for (Object entry : entries) {
            String header = (String) entry;
            int separator = header.indexOf(':');
            if (separator <= 0 || header.substring(0, separator).isBlank()) {
                throw new IllegalArgumentException(String.format(
                        "Header '%s' in '%s' must be in the form 'Name:Value'", header, name));
            }
            headers.put(header.substring(0, separator).trim(), header.substring(separator + 1).trim());
        }
        return headers;
    }

    public enum BehaviorOnMalformedDoc {
        IGNORE,
        WARN,
        FAIL
    }

    public enum OperationalMode {
        UPSERT,
        RAW
    }
}
