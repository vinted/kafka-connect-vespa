package com.vinted.kafka.connect.vespa;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.Validators;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;

public class VespaSinkConfig extends AbstractConfig {
    // Connector group
    public static final String ENDPOINT_CONFIG = "vespa.endpoint";
    private static final String ENDPOINT_DOC = "The comma-separated list of one or more Vespa URLs, such as "
            + "``https://node1:8080,http://node2:8080`` or ``https://node3:8080``. HTTPS is used for all connections "
            + "if any of the URLs starts with ``https:``. A URL without a protocol is treated as "
            + "``http``.";
    private static final String ENDPOINT_DISPLAY = "Endpoint URLs";
    private static final String ENDPOINT_DEFAULT = "http://localhost:8080";

    public static final String CONNECTIONS_PER_ENDPOINT_CONFIG = "vespa.connections.per.endpoint";
    private static final String CONNECTIONS_PER_ENDPOINT_DOC = "A reasonable value here is a value that lets all feed "
            + "clients (if more than one) Sets the number of connections this client will use "
            + "collectively have a number of connections which is a small multiple of the numbers of containers in "
            + "the cluster to feed, so load can be balanced across these containers. In general, this value should "
            + "be kept as low as possible, but poor connectivity between feeder and cluster may also warrant a higher "
            + "number of connections.";
    private static final String CONNECTIONS_PER_ENDPOINT_DISPLAY = "Connections per endpoint";

    private static final int CONNECTIONS_PER_ENDPOINT_DEFAULT = 8;

    public static final String MAX_STREAMS_PER_CONNECTION_CONFIG = "vespa.max.streams.per.connection";
    private static final String MAX_STREAMS_PER_CONNECTION_DOC = "This determines the maximum number of concurrent, "
            + "in-flight requests for this. Sets the maximum number of streams per HTTP/2 "
            + "client, which is maxConnections * maxStreamsPerConnection. Prefer more streams over more connections, "
            + "when possible. The feed client automatically throttles load to achieve the best throughput, and the "
            + "actual number of streams per connection is usually lower than the maximum.";
    private static final String MAX_STREAMS_PER_CONNECTION_DISPLAY = "Max streams per connection";
    private static final int MAX_STREAMS_PER_CONNECTION_DEFAULT = 128;

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
    private static final String MAX_FAILURE_MS_DOC = "The period of consecutive failures before shutting down.";
    private static final String MAX_FAILURE_MS_DISPLAY = "Max failure ms";
    private static final int MAX_FAILURE_MS_DEFAULT = 60000;

    public static final String NAMESPACE_CONFIG = "vespa.namespace";
    private static final String NAMESPACE_DOC = "User specified part of each document ID in that sense. Namespace can "
            + "not be used in queries, other than as part of the full document ID. However, it can be used for "
            + "document selection, where id.namespace can be accessed and compared to a given string, for instance. "
            + "An example use case is visiting a subset of documents. Defaults to topic name if not specified.";
    private static final String NAMESPACE_DISPLAY = "Namespace";
    private static final String NAMESPACE_DEFAULT = null;

    public static final String DOCUMENT_TYPE_CONFIG = "vespa.document.type";
    private static final String DOCUMENT_TYPE_DOC = "Document type as defined in services.xml and the schema. "
            + "Defaults to topic name if not specified";
    private static final String DOCUMENT_TYPE_DISPLAY = "Document type";
    private static final String DOCUMENT_TYPE_DEFAULT = null;

    public static final String OPERATIONAL_MODE_CONFIG = "vespa.operational.mode";
    private static final String OPERATIONAL_MODE_DOC = "The operational mode of the connector. Valid options are "
            + "upsert and raw. Upsert mode will update existing documents and insert new documents, tombstones "
            + "messages will be converted to delete operations. Raw mode executes all operations using document "
            + "json format as explained in https://docs.vespa.ai/en/reference/document-json-format.html.";
    private static final String OPERATIONAL_MODE_DISPLAY = "Operational mode";
    private static final OperationalMode OPERATIONAL_MODE_DEFAULT = OperationalMode.UPSERT;

    // Operation group
    public static final String OPERATION_RETRIES_CONFIG = "vespa.operation.retries";
    private static final String OPERATION_RETRIES_DOC = "Number of retries per operation for assumed transient, "
            + "non-backpressure problems.";
    private static final String OPERATION_RETRIES_DISPLAY = "Max Retries";
    private static final int OPERATION_RETRIES_DEFAULT = 10;

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

    // Data conversion group

    public static final String DROP_INVALID_MESSAGE_CONFIG = "vespa.drop.invalid.message";
    private static final String DROP_INVALID_MESSAGE_DOC = "Whether to drop kafka message when it cannot be "
            + "converted to output message.";
    private static final String DROP_INVALID_MESSAGE_DISPLAY = "Drop invalid messages";
    private static final boolean DROP_INVALID_MESSAGE_DEFAULT = false;

    public static final String BEHAVIOR_ON_MALFORMED_DOCS_CONFIG = "vespa.behavior.on.malformed.documents";
    private static final String BEHAVIOR_ON_MALFORMED_DOCS_DOC = "How to handle records that Vespa rejects due to "
            + "document malformation. Valid options are `ignore`, `warn`, and `fail`.";
    private static final String BEHAVIOR_ON_MALFORMED_DOCS_DISPLAY = "Behavior on malformed documents";
    private static final BehaviorOnMalformedDoc BEHAVIOR_ON_MALFORMED_DOCS_DEFAULT = BehaviorOnMalformedDoc.FAIL;

    private static final String CONNECTOR_GROUP = "Connector";
    private static final String OPERATION_GROUP = "Operation";
    private static final String DATA_CONVERSION_GROUP = "Data Conversion";

    public static final ConfigDef CONFIG = baseConfigDef();

    protected static ConfigDef baseConfigDef() {
        final ConfigDef configDef = new ConfigDef();
        addConnectorConfigs(configDef);
        addOperationConfigs(configDef);
        addDataConversionConfigs(configDef);
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
                        OPERATIONAL_MODE_DISPLAY);
    }

    private static void addOperationConfigs(ConfigDef configDef) {
        int order = 0;
        configDef
                .define(
                        OPERATION_RETRIES_CONFIG,
                        ConfigDef.Type.INT,
                        OPERATION_RETRIES_DEFAULT,
                        between(0, Integer.MAX_VALUE),
                        ConfigDef.Importance.LOW,
                        OPERATION_RETRIES_DOC,
                        OPERATION_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        OPERATION_RETRIES_DISPLAY)
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
                        OPERATION_TRACELEVEL_DISPLAY);
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

    public final int retries;
    public final int connectionsPerEndpoint;
    public final int maxStreamsPerConnection;
    public final boolean dryrun;
    public final boolean speedTest;
    public final Duration maxFailureDuration;
    public final String namespace;
    public final String documentType;
    public final String route;
    public final Duration timeout;
    public final int tracelevel;
    public final Set<URI> urls;
    public final boolean dropInvalidMessage;
    public final BehaviorOnMalformedDoc behaviorOnMalformedDoc;
    public final OperationalMode operationalMode;

    public VespaSinkConfig(Map<String, ?> configProviderProps) {
        super(CONFIG, configProviderProps);

        this.retries = getInt(OPERATION_RETRIES_CONFIG);
        this.connectionsPerEndpoint = getInt(CONNECTIONS_PER_ENDPOINT_CONFIG);
        this.maxStreamsPerConnection = getInt(MAX_STREAMS_PER_CONNECTION_CONFIG);
        this.dryrun = getBoolean(DRYRUN_CONFIG);
        this.speedTest = getBoolean(SPEED_TEST_CONFIG);
        this.maxFailureDuration = Duration.ofMillis(getInt(MAX_FAILURE_MS_CONFIG));
        this.namespace = getString(NAMESPACE_CONFIG);
        this.documentType = getString(DOCUMENT_TYPE_CONFIG);
        this.route = getString(OPERATION_ROUTE_CONFIG);
        this.timeout = Duration.ofMillis(getInt(OPERATION_TIMEOUT_MS_CONFIG));
        this.tracelevel = getInt(OPERATION_TRACELEVEL_CONFIG);
        this.urls = getList(ENDPOINT_CONFIG)
                .stream()
                .map(s -> s.endsWith("/") ? s : s + "/")
                .map(URI::create)
                .collect(Collectors.toCollection(HashSet::new));
        this.dropInvalidMessage = getBoolean(DROP_INVALID_MESSAGE_CONFIG);
        this.behaviorOnMalformedDoc = ConfigUtils.getEnum(BehaviorOnMalformedDoc.class, this, BEHAVIOR_ON_MALFORMED_DOCS_CONFIG);
        this.operationalMode = ConfigUtils.getEnum(OperationalMode.class, this, OPERATIONAL_MODE_CONFIG);
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
