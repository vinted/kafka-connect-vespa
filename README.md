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

#### RAW

Raw mode will execute kafka messages as vespa document api operations using document JSON format. If you want those
operations to be executed in order, make sure they have a unique kafka key so that the operations are sent to the same
kafka partitions. In this mode, connector does not use the kafka keys at all.

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

`vespa.cloud.token`

Vespa Cloud token when using token based authentication.

* Type: string
* Default: null
* Valid Values: Matches: non-empty string
* Importance: low

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
