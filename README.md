# Kafka Connect Vespa Sink Connector

## Introduction

The Vespa Sink Connector is used to write data from Kafka to a Vespa search engine.

### Important

This connector expects records from Kafka to have a key and value. Values can be converted using byte, string or JSON
converters. Topic names are used as document types, use single message transforms to transform topic names into desired
document types.

### Note

This connector supports deletes. If the record stored in Kafka has a null value, this connector will delete document
with the corresponding key to Vespa.

### Configuration

# Connector

`vespa.endpoint`  
The comma-separated list of one or more Vespa URLs, such as
`https://node1:8080,http://node2:8080` or `https://node3:8080`. HTTPS is
used for all connections if any of the URLs starts with `https:`. A URL
without a protocol is treated as `http`.

- Type: list
- Default: <http://localhost:8080>
- Valid Values:
- Importance: high

`vespa.connections.per.endpoint`  
A reasonable value here is a value that lets all feed clients (if more
than one) Sets the number of connections this client will use
collectively have a number of connections which is a small multiple of
the numbers of containers in the cluster to feed, so load can be
balanced across these containers. In general, this value should be kept
as low as possible, but poor connectivity between feeder and cluster may
also warrant a higher number of connections.

- Type: int
- Default: 8
- Valid Values: \[1,...\]
- Importance: low

`vespa.max.streams.per.connection`  
This determines the maximum number of concurrent, inflight requests for
this Sets the maximum number of streams per HTTP/2 client, which is
maxConnections \* maxStreamsPerConnection. Prefer more streams over more
connections, when possible. The feed client automatically throttles load
to achieve the best throughput, and the actual number of streams per
connection is usually lower than the maximum.

- Type: int
- Default: 32
- Valid Values: \[1,...\]
- Importance: low

`vespa.dryrun`  
Turns on dryrun mode, where each operation succeeds after a given delay,
rather than being sent across the network.

- Type: boolean
- Default: false
- Importance: low

`vespa.speedtest`  
Turns on speed test mode, where each operation succeeds immediately,
rather than being sent across the network.

- Type: boolean
- Default: false
- Importance: low

`vespa.max.failure.ms`  
The period of consecutive failures before shutting down.

- Type: int
- Default: 60000 (1 minute)
- Valid Values: \[10000,...\]
- Importance: low

`vespa.namespace`  
User specified part of each document ID in that sense. Namespace can not
be used in queries, other than as part of the full document ID. However,
it can be used for document selection, where id.namespace can be
accessed and compared to a given string, for instance. An example use
case is visiting a subset of documents.

- Type: string
- Default: mynamespace
- Valid Values: non-empty string without ISO control characters
- Importance: high

`vespa.document.type`  
Document type as defined in services.xml and the schema.

- Type: string
- Default: mydocumenttype
- Valid Values: non-empty string without ISO control characters
- Importance: high

`vespa.operational.mode`  
The operational mode of the connector. Valid options are upsert and raw.
Upsert mode will update existing documents and insert new documents,
tombstones messages will be converted to delete operations. Raw mode
executes all operations using document json format as explained in
<https://docs.vespa.ai/en/reference/document-json-format.html>.

- Type: string
- Default: UPSERT
- Valid Values: Matches: `UPSERT`, `RAW`
- Importance: high

# Operation

`vespa.operation.retries`  
Number of retries per operation for assumed transient, non-backpressure
problems.

- Type: int
- Default: 10
- Valid Values: \[0,...,2147483647\]
- Importance: low

`vespa.operation.timeout.ms`  
Feed operation timeout.

- Type: int
- Default: 60000 (1 minute)
- Valid Values: \[0,...,2147483647\]
- Importance: low

`vespa.operation.route`  
Target Vespa route for feed operations.

- Type: string
- Default: null
- Valid Values: non-empty string without ISO control characters
- Importance: low

`vespa.operation.tracelevel`  
The trace level of network traffic.

- Type: int
- Default: 0
- Valid Values: \[0,...,9\]
- Importance: low

# Data Conversion

`vespa.drop.invalid.message`  
Whether to drop kafka message when it cannot be converted to output
message.

- Type: boolean
- Default: false
- Importance: low

`vespa.behavior.on.malformed.documents`  
How to handle records that Vespa rejects due to document malformation.
Valid options are ignore', 'warn', and 'fail'.

- Type: string
- Default: FAIL
- Valid Values: Matches: `IGNORE`, `WARN`, `FAIL`
- Importance: low

#### Examples

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
vespa.namespace=mynamespace
vespa.document.type=mydocumenttype
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
    "vespa.namespace": "mynamespace",
    "vespa.document.type": "mydocumenttype"
  }
}
```

Use curl to post the configuration to one of the Kafka Connect Workers. Change `http://localhost:8088/` the endpoint of
one of your Kafka Connect worker(s).

Create a new instance.

```bash
curl -s -X POST -H 'Content-Type: application/json' --data @connector.json http://localhost:8088/connectors
```

Update an existing instance.

```bash
curl -s -X PUT -H 'Content-Type: application/json' --data @connector.json http://localhost:8088/connectors/TestSinkConnector1/config
```
