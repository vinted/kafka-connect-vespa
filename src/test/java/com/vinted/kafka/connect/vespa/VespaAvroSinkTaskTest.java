package com.vinted.kafka.connect.vespa;

import ai.vespa.feed.client.DocumentId;
import com.vinted.kafka.connect.vespa.mocks.FakeFeedClient;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertNothingSent;
import static com.vinted.kafka.connect.vespa.mocks.FeedAssertions.assertSent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VespaAvroSinkTaskTest {
    private static final String TOPIC = "music";

    private static final Schema ALBUM = new Schema.Parser().parse("{"
            + "\"type\":\"record\",\"name\":\"Album\",\"namespace\":\"test\",\"fields\":["
            + "{\"name\":\"title\",\"type\":\"string\"},"
            + "{\"name\":\"year\",\"type\":\"int\"},"
            + "{\"name\":\"rating\",\"type\":[\"null\",\"double\"],\"default\":null},"
            + "{\"name\":\"price\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}},"
            + "{\"name\":\"released\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
            + "{\"name\":\"updated_at\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
            + "{\"name\":\"genre\",\"type\":{\"type\":\"enum\",\"name\":\"Genre\",\"symbols\":[\"ROCK\",\"JAZZ\"]}},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
            + "{\"name\":\"attributes\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
            + "{\"name\":\"artist\",\"type\":{\"type\":\"record\",\"name\":\"Artist\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}},"
            + "{\"name\":\"cover\",\"type\":\"bytes\"}"
            + "]}");

    private static final Schema RAW_PUT = new Schema.Parser().parse("{"
            + "\"type\":\"record\",\"name\":\"Put\",\"namespace\":\"test\",\"fields\":["
            + "{\"name\":\"put\",\"type\":\"string\"},"
            + "{\"name\":\"fields\",\"type\":{\"type\":\"record\",\"name\":\"Fields\",\"fields\":["
            + "{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"year\",\"type\":\"int\"}]}}"
            + "]}");

    private static final Schema KEY = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"Key\",\"namespace\":\"test\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");

    private final Map<String, String> registry = Map.of("schema.registry.url", "mock://" + UUID.randomUUID());
    private final KafkaAvroSerializer keySerializer = new KafkaAvroSerializer();
    private final KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer();
    private final AvroConverter keyConverter = new AvroConverter();
    private final AvroConverter valueConverter = new AvroConverter();

    private final Map<String, String> params = new HashMap<>();
    private final FakeFeedClient client = new FakeFeedClient();
    private final ConnectWorker worker = new ConnectWorker(new VespaSinkTask(config -> client));
    private long offset;

    @BeforeEach
    void before() {
        keySerializer.configure(registry, true);
        valueSerializer.configure(registry, false);
        keyConverter.configure(registry, true);
        valueConverter.configure(registry, false);

        params.put(VespaSinkConfig.NAMESPACE_CONFIG, "ns");
        params.put(VespaSinkConfig.DOCUMENT_TYPE_CONFIG, "album");
    }

    @AfterEach
    void after() {
        worker.stop();
    }

    @Test
    void putsAvroRecordsAsVespaFields() {
        worker.start(params);

        worker.put(List.of(record("album-1", album())));

        assertSent(client, "put id:ns:album::album-1 {\"fields\":{"
                + "\"title\":\"Abbey Road\","
                + "\"year\":1969,"
                + "\"rating\":null,"
                + "\"price\":12.50,"
                + "\"released\":-97,"
                + "\"updated_at\":1742837807123,"
                + "\"genre\":\"ROCK\","
                + "\"tags\":[\"classic\",\"remastered\"],"
                + "\"attributes\":{\"label\":\"Apple\"},"
                + "\"artist\":{\"name\":\"The Beatles\"},"
                + "\"cover\":\"aGk=\""
                + "}}");
    }

    @Test
    void removesDocumentsOnAvroTombstones() {
        worker.start(params);

        worker.put(List.of(record("album-1", null)));

        assertSent(client, "remove id:ns:album::album-1");
    }

    @Test
    void usesAvroLongKeysAsDocumentIds() {
        worker.start(params);

        worker.put(List.of(record(42L, album())));

        assertEquals(List.of(DocumentId.of("id:ns:album::42")),
                client.operations().stream().map(operation -> operation.documentId).collect(Collectors.toList()));
    }

    @Test
    void treatsAvroRecordKeysAsInvalidMessages() {
        GenericRecord key = new GenericData.Record(KEY);
        key.put("id", "album-1");
        worker.start(params);

        assertThrows(ConnectException.class, () -> worker.put(List.of(record(key, album()))));
        assertNothingSent(client);
    }

    @Test
    void feedsAvroRecordsAsRawOperations() {
        params.put(VespaSinkConfig.OPERATIONAL_MODE_CONFIG, VespaSinkConfig.OperationalMode.RAW.name());
        GenericRecord fields = new GenericData.Record(RAW_PUT.getField("fields").schema());
        fields.put("title", "Abbey Road");
        fields.put("year", 1969);
        GenericRecord put = new GenericData.Record(RAW_PUT);
        put.put("put", "id:ns:album::album-1");
        put.put("fields", fields);
        worker.start(params);

        worker.put(List.of(record("album-1", put)));

        assertSent(client, "put id:ns:album::album-1 {\"fields\":{\"title\":\"Abbey Road\",\"year\":1969}}");
    }

    private SinkRecord record(Object key, Object value) {
        SchemaAndValue connectKey = keyConverter.toConnectData(TOPIC, keySerializer.serialize(TOPIC, key));
        SchemaAndValue connectValue = valueConverter.toConnectData(TOPIC, valueSerializer.serialize(TOPIC, value));
        return new SinkRecord(TOPIC, 0, connectKey.schema(), connectKey.value(),
                connectValue.schema(), connectValue.value(), offset++);
    }

    private static GenericRecord album() {
        Schema price = ALBUM.getField("price").schema();

        GenericRecord artist = new GenericData.Record(ALBUM.getField("artist").schema());
        artist.put("name", "The Beatles");

        GenericRecord album = new GenericData.Record(ALBUM);
        album.put("title", "Abbey Road");
        album.put("year", 1969);
        album.put("rating", null);
        album.put("price", new Conversions.DecimalConversion().toBytes(new BigDecimal("12.50"), price, LogicalTypes.decimal(10, 2)));
        album.put("released", -97);
        album.put("updated_at", 1742837807123L);
        album.put("genre", new GenericData.EnumSymbol(ALBUM.getField("genre").schema(), "ROCK"));
        album.put("tags", List.of("classic", "remastered"));
        album.put("attributes", Map.of("label", "Apple"));
        album.put("artist", artist);
        album.put("cover", ByteBuffer.wrap("hi".getBytes(StandardCharsets.UTF_8)));
        return album;
    }
}
