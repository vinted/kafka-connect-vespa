# Kafka Connect Vespa Sink Connector

## Introduction

The Vespa Sink Connector is used to write data from Kafka to a Vespa search engine.

### Installation

This connector has not yet been published to Confluent Hub. To install it, download the latest component package and
install it using `confluent-hub` command line tool.

```sh
wget https://github.com/vinted/kafka-connect-vespa/releases/download/v1.0.11/vinted-kafka-connect-vespa-v1.0.11.zip -O /tmp/vinted-kafka-connect-vespa-v1.0.11.zip -q
```

```sh
confluent-hub install --no-prompt /tmp/vinted-kafka-connect-vespa-v1.0.11.zip
```

### Operational modes

The connector can work in two operational modes which act very differently and serve two distinct cases. It can be
configured using the `vespa.operational.mode` configuration parameter.

#### UPSERT

Upsert mode will replace existing documents, tombstones messages will be converted to delete operations. Document keys
are constructed using the following format: `namespace:documenttype:id`. By default, the topic names are used for both
the namespace and the document type, but they can be overriden using the `vespa.namespace` and `vespa.document.type`
configuration properties. If you need something more complex, consider using single message transforms to change the
topic name.

Within a batch, only the last record for each document is sent to Vespa. `vespa.operation.condition` applies to puts and
removes, and `vespa.operation.create.if.non.existent` only to puts.

#### RAW

Raw mode will execute kafka messages as vespa document api operations using document JSON format. If you want those
operations to be executed in order, make sure they have a unique kafka key so that the operations are sent to the same
kafka partitions. In this mode, connector does not use the kafka keys at all. Every operation is sent, in order, since an
update can depend on an earlier operation on the same document. Each operation carries its own id, `condition` and `create`,
so `vespa.namespace`, `vespa.document.type`, `vespa.operation.condition` and `vespa.operation.create.if.non.existent` are
ignored.

Tombstones carry no operation and, without the kafka key, no document id, so they are skipped. To delete documents, send
a `remove` operation.

A value the connector cannot convert to JSON is an invalid message, handled according to `vespa.drop.invalid.message`
as in UPSERT mode. A value that converts but is not a valid document JSON operation is a malformed document, handled
according to `vespa.behavior.on.malformed.documents`.

### Data formats

Records are deserialized by the converters configured on the worker or the connector (`key.converter`,
`value.converter`), and the connector turns each value into Vespa document JSON. Values can be strings or bytes holding
JSON, which are passed through as-is, or schema-based data from any converter, such as JSON, Avro, Protobuf or JSON
Schema, which is converted to JSON.

#### Avro with Schema Registry

The connector bundles Confluent's `AvroConverter`, so it works on any Kafka Connect worker, with or without Confluent
Platform:

```json
"key.converter": "io.confluent.connect.avro.AvroConverter",
"key.converter.schema.registry.url": "http://schema-registry:8081",
"value.converter": "io.confluent.connect.avro.AvroConverter",
"value.converter.schema.registry.url": "http://schema-registry:8081"
```

Converter options such as Schema Registry authentication go under the same prefix, see
[Confluent's documentation](https://docs.confluent.io/platform/current/schema-registry/connect.html). A
[complete example](config/quickstart-vespa-avro.json) is in `config/`.

In UPSERT mode the record becomes the document's `fields`; in RAW mode it must have the shape of a Vespa document
operation, such as `put` and `fields`. Avro types map to Vespa JSON as follows:

| Avro | Vespa JSON |
|---|---|
| record | struct, as an object |
| array | array, or a dense tensor for numbers |
| map | map or weighted set, or a mapped tensor for numbers |
| enum, string, uuid | string |
| bytes, fixed | base64 string, as Vespa `raw` expects |
| decimal | exact number; use a Vespa `double` or `string` field |
| date | days since the epoch |
| time-millis | milliseconds since midnight |
| timestamp-millis | milliseconds since the epoch |
| timestamp-micros, time-micros, local-timestamp-millis | the raw long, in the unit of the logical type |
| optional field set to null | left out of the document |

Vespa has no date type, so dates and times are written as numbers; use the `TimestampConverter` single message
transform to change their unit. Unions of more than one non-null type, and Avro fields that are not in the Vespa schema,
are rejected by Vespa as malformed documents; drop such fields with the `ReplaceField` transform or set Vespa's
`ignore-undefined-fields`.

Keys must be strings or integers, as with any converter. A record key is an invalid message; extract a field with the
`ExtractField$Key` transform instead.

### Configuration

#### Connector

`vespa.endpoint`

The comma-separated list of one or more Vespa URLs, such as `https://node1:8080,http://node2:8080`
or `https://node3:8080`. HTTPS is used for all connections if any of the URLs starts with `https:`. A URL without a
protocol is treated as `http`.

* Type: list
* Default: http://localhost:8080
* Valid Values:
* Importance: high

`vespa.connections.per.endpoint`

A reasonable value here is a value that lets all feed clients (if more than one) Sets the number of connections this
client will use collectively have a number of connections which is a small multiple of the numbers of containers in the
cluster to feed, so load can be balanced across these containers. In general, this value should be kept as low as
possible, but poor connectivity between feeder and cluster may also warrant a higher number of connections.

* Type: int
* Default: 1
* Valid Values: [1,...]
* Importance: low

`vespa.max.streams.per.connection`

This determines the maximum number of concurrent, in-flight requests for this. Sets the maximum number of streams per
HTTP/2 client, which is maxConnections * maxStreamsPerConnection. Prefer more streams to more connections, when
possible. The feed client automatically throttles load to achieve the best throughput, and the actual number of streams
per connection is usually lower than the maximum.

* Type: int
* Default: 16
* Valid Values: [1,...]
* Importance: low

`vespa.dryrun`

Turns on dryrun mode, where each operation succeeds after a given delay, rather than being sent across the network.

* Type: boolean
* Default: false
* Importance: low

`vespa.speedtest`
Turns on speed test mode, where each operation succeeds immediately, rather than being sent across the network.

* Type: boolean
* Default: false
* Importance: low

`vespa.max.failure.ms`

The period of consecutive failures before shutting down.

* Type: int
* Default: 60000 (1 minute)
* Valid Values: [10000,...]
* Importance: low

`vespa.namespace`

User specified part of each document ID in that sense. Namespace can not be used in queries, other than as part of the
full document ID. However, it can be used for document selection, where namespace can be accessed and compared to a
given string, for instance. An example use case is visiting a subset of documents. Defaults to topic name if not
specified.

* Type: string
* Default: null
* Valid Values: non-empty string without ISO control characters
* Importance: high

`vespa.document.type`

Document type as defined in services.xml and the schema. Defaults to topic name if not specified

* Type: string
* Default: null
* Valid Values: non-empty string without ISO control characters
* Importance: high

`vespa.operational.mode`

The operational mode of the connector. Valid options are upsert and raw. Upsert mode will update existing documents and
insert new documents, tombstones messages will be converted to delete operations. Raw mode executes all operations using
document json format as explained in https://docs.vespa.ai/en/reference/document-json-format.html.

* Type: string
* Default: UPSERT
* Valid Values: Matches: `UPSERT`, `RAW`
* Importance: high

`vespa.retry.strategy.retries`

Number of retries per operation for assumed transient, non-backpressure problems.

* Type: int
* Default: 10
* Valid Values: [0,...,2147483647]
* Importance: low

`vespa.retry.strategy.operation.types`

Operation types to retry.

* Type: list
* Default: PUT,UPDATE,REMOVE
* Valid Values: Matches: `PUT`, `UPDATE`, `REMOVE`
* Importance: low

`vespa.max.retries`

How many times in a row to retry a batch that failed only because Vespa or the network was unavailable, such as during
a Vespa upgrade: network errors, timeouts, server errors (5xx) and responses that are not Vespa JSON. Client errors
(4xx) are never retried. Between retries the task pauses its partitions for a random backoff between 0 and
`vespa.retry.backoff.ms * 2^retry`, also capped by the time left until the next offset commit, then redelivers the
whole batch. The count resets after every successful batch; once it is exhausted the task fails. These retries come on
top of the feed client's own immediate retries, `vespa.retry.strategy.retries`.

* Type: int
* Default: 10
* Valid Values: [0,...]
* Importance: medium

`vespa.retry.backoff.ms`

Initial backoff before retrying a batch after Vespa or the network was unavailable. Each retry waits a random time
between 0 and twice the previous upper bound. See `vespa.max.retries`.

* Type: long
* Default: 1000 (1 second)
* Valid Values: [0,...]
* Importance: medium

`vespa.circuit.breaker.grace.period.ms`

The period of consecutive failures before the circuit breaker changes state to half-open, during which the feed client
sends only a single request at a time to probe the cluster. See `vespa.max.failure.ms` for the period after which the
client shuts down entirely.

* Type: long
* Default: 10000 (10 seconds)
* Valid Values: [0,...]
* Importance: low

`vespa.connection.ttl.ms`

Duration after which the feed client recycles active connections. Useful for rebalancing connections behind a load
balancer. `0` disables recycling.

* Type: long
* Default: 0
* Valid Values: [0,...]
* Importance: low

`vespa.compression`

Compression to use for request bodies. `AUTO` compresses large request bodies only.

* Type: string
* Default: AUTO
* Valid Values: (case insensitive) `AUTO`, `NONE`, `GZIP`
* Importance: low

`vespa.initial.inflight.factor`

Initial inflight factor of the feed client's throttler. The initial target of inflight operations is
`minInflight * initialInflightFactor`; the throttler then adjusts it based on observed throughput.

* Type: int
* Default: 1
* Valid Values: [1,...]
* Importance: low

`vespa.benchmark`

Whether the feed client collects operation statistics. The connector does not read them, so disabling this saves a
little overhead.

* Type: boolean
* Default: true
* Importance: low

`vespa.request.headers`

Additional HTTP headers sent with every request to Vespa, as a comma-separated list of `Name:Value` pairs, such as
`X-Team:search,X-Env:prod`. Header values cannot contain commas. Values are logged in plain text, so use
`vespa.cloud.token` for the Vespa Cloud token.

* Type: list
* Default: ""
* Valid Values: comma-separated list of `Name:Value` pairs
* Importance: low

`vespa.cloud.token`

Vespa Cloud token when using token based authentication. Sent as an `Authorization: Bearer` header and masked in logs.

* Type: password
* Default: null
* Valid Values: non-empty string
* Importance: low

#### Operation

`vespa.operation.timeout.ms`

Feed operation timeout.

* Type: int
* Default: 60000 (1 minute)
* Valid Values: [0,...,2147483647]
* Importance: low

`vespa.operation.route`

Target Vespa route for feed operations.

* Type: string
* Default: null
* Valid Values: non-empty string without ISO control characters
* Importance: low

`vespa.operation.tracelevel`

The trace level of network traffic.

* Type: int
* Default: 0
* Valid Values: [0,...,9]
* Importance: low

`vespa.operation.condition`

Test-and-set condition applied to every put and remove in UPSERT mode, such as `music.locked == false`. Operations whose
condition is not met are handled according to `vespa.behavior.on.malformed.documents`. Ignored in RAW mode, where each
operation carries its own `condition`.

* Type: string
* Default: null
* Valid Values: non-empty string without ISO control characters
* Importance: low

`vespa.operation.create.if.non.existent`

In UPSERT mode, create the document when a conditional put targets a document that does not exist, instead of failing
the condition. Only meaningful together with `vespa.operation.condition`. Ignored in RAW mode, where each operation
carries its own `create`.

* Type: boolean
* Default: false
* Importance: low

#### Data Conversion

`vespa.drop.invalid.message`

Whether to drop kafka message when it cannot be converted to output message.

* Type: boolean
* Default: false
* Importance: low

`vespa.behavior.on.malformed.documents`

How to handle records that Vespa rejects due to document malformation. Valid options are `IGNORE`, `WARN`, and `FAIL`.

* Type: string
* Default: FAIL
* Valid Values: Matches: `IGNORE`, `WARN`, `FAIL`
* Importance: low

#### Security

`vespa.ssl.certificate.path`

Path to a PEM file with the client certificate used for mutual TLS, such as the data plane certificate of a Vespa Cloud
application. Must be set together with `vespa.ssl.private.key.path`.

* Type: string
* Default: null
* Valid Values: non-empty string without ISO control characters
* Importance: medium

`vespa.ssl.private.key.path`

Path to a PEM file with the private key of the client certificate. Must be set together with
`vespa.ssl.certificate.path`.

* Type: string
* Default: null
* Valid Values: non-empty string without ISO control characters
* Importance: medium

`vespa.ssl.ca.certificates.path`

Path to a PEM file with trusted CA certificates. Overrides the JVM default truststore.

* Type: string
* Default: null
* Valid Values: non-empty string without ISO control characters
* Importance: low

`vespa.ssl.hostname.verification.enabled`

Whether to verify that the Vespa endpoint hostname matches its TLS certificate. Disable only for testing.

* Type: boolean
* Default: true
* Importance: low

#### Proxy

`vespa.proxy.url`

HTTP or HTTPS proxy URL used for all endpoints, such as `http://proxy:3128`.

* Type: string
* Default: null
* Valid Values: http or https URL
* Importance: low

`vespa.proxy.ca.certificates.path`

Path to a PEM file with trusted CA certificates for an HTTPS proxy. Overrides the JVM default truststore.

* Type: string
* Default: null
* Valid Values: non-empty string without ISO control characters
* Importance: low

`vespa.proxy.hostname.verification.enabled`

Whether to verify that the HTTPS proxy hostname matches its TLS certificate. Disable only for testing.

* Type: boolean
* Default: true
* Importance: low

`vespa.proxy.request.headers`

Additional HTTP headers sent with every request to the proxy, as a comma-separated list of `Name:Value` pairs, such as
`Proxy-Authorization:Basic abc`. Header values cannot contain commas and are logged in plain text.

* Type: list
* Default: ""
* Valid Values: comma-separated list of `Name:Value` pairs
* Importance: low

### Authentication

#### Mutual TLS

To authenticate with a client certificate, as Vespa Cloud data plane access or a self-hosted Vespa with
[mTLS](https://docs.vespa.ai/en/mtls.html) requires, point the connector at PEM files available on every Kafka Connect
worker:

```properties
vespa.endpoint=https://my-app.my-tenant.aws-us-east-1c.z.vespa-app.cloud/
vespa.ssl.certificate.path=/etc/kafka-connect/vespa/data-plane-public-cert.pem
vespa.ssl.private.key.path=/etc/kafka-connect/vespa/data-plane-private-key.pem
# Only needed when the server certificate is not signed by a CA in the JVM default truststore.
vespa.ssl.ca.certificates.path=/etc/kafka-connect/vespa/ca.pem
```

See [config/quickstart-vespa-mtls.json](config/quickstart-vespa-mtls.json) for a full example.

#### Token

For Vespa Cloud token authentication, set `vespa.cloud.token`.

#### Examples

Connector configuration examples can be found in the [config](config) directory. But here are some quick ones to get
started.

##### Standalone Example

This configuration is used typically along
with [standalone mode](http://docs.confluent.io/current/connect/concepts.html#standalone-workers).

```properties
name=VespaSinkConnector1
connector.class=com.vinted.kafka.connect.vespa.VespaSinkConnector
tasks.max=1
topics=music
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
vespa.endpoint=http://vespa:8080/
vespa.namespace=default
vespa.document.type=music
```

##### Distributed Example

This configuration is used typically along
with [distributed mode](http://docs.confluent.io/current/connect/concepts.html#distributed-workers). Write the following
json to `connector.json`, configure all the required values, and use the command below to post the configuration to one
the distributed connect worker(s).

```json
{
  "config": {
    "connector.class": "com.vinted.kafka.connect.vespa.VespaSinkConnector",
    "tasks.max": "1",
    "topics": "music",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "vespa.endpoint": "http://vespa:8080/",
    "vespa.namespace": "default",
    "vespa.document.type": "music"
  }
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8083/` the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.

```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors
```

Update an existing instance.

```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8083/connectors/TestSinkConnector1/config
```
