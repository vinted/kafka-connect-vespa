package com.vinted.kafka.connect.vespa;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Generated marketplace items of about 1 KB, as records in the shapes a Connect converter hands a sink task. */
final class PerformanceRecords {
    static final String TOPIC = "items";
    private static final int PARTITIONS = 6;
    private static final String[] WORDS = ("vintage dress cotton blue leather jacket size small medium large summer "
            + "winter wool shoes boots black white red green bag handmade new worn once perfect condition brand "
            + "original classic retro denim jeans shirt silk scarf gold silver ring necklace watch").split(" ");
    private static final String[] BRANDS = {"Zara", "H&M", "Nike", "Adidas", "Levi's", "Mango", "Uniqlo", "Gucci"};
    private static final Schema VALUE = SchemaBuilder.struct().name("item")
            .field("title", Schema.STRING_SCHEMA)
            .field("description", Schema.STRING_SCHEMA)
            .field("brand", Schema.STRING_SCHEMA)
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("price", Schema.FLOAT64_SCHEMA)
            .field("views", Schema.INT64_SCHEMA)
            .field("locked", Schema.BOOLEAN_SCHEMA)
            .build();

    private PerformanceRecords() {
    }

    interface Factory {
        SinkRecord record(Document document, long offset);

        default List<SinkRecord> records(List<Document> documents, long firstOffset) {
            List<SinkRecord> records = new ArrayList<>(documents.size());
            for (int i = 0; i < documents.size(); i++) {
                records.add(record(documents.get(i), firstOffset + i));
            }
            return records;
        }
    }

    static final class Document {
        final String key;
        final String title;
        final String description;
        final String brand;
        final List<String> tags;
        final double price;
        final long views;
        final boolean locked;
        final String json;

        private Document(int id, Random random) {
            this.key = "item-" + id;
            this.title = words(random, 6);
            this.description = words(random, 130);
            this.brand = BRANDS[random.nextInt(BRANDS.length)];
            this.tags = IntStream.range(0, 5).mapToObj(i -> WORDS[random.nextInt(WORDS.length)]).collect(Collectors.toList());
            this.price = Math.round(random.nextDouble() * 50_000) / 100.0;
            this.views = random.nextInt(100_000);
            this.locked = random.nextBoolean();
            this.json = String.format("{\"title\":\"%s\",\"description\":\"%s\",\"brand\":\"%s\",\"tags\":[%s],"
                            + "\"price\":%s,\"views\":%d,\"locked\":%b}",
                    title, description, brand, tags.stream().map(tag -> "\"" + tag + "\"").collect(Collectors.joining(",")),
                    price, views, locked);
        }
    }

    static List<Document> documents(int count) {
        Random random = new Random(42);
        return IntStream.range(0, count).mapToObj(id -> new Document(id, random)).collect(Collectors.toList());
    }

    static SinkRecord jsonString(Document document, long offset) {
        return record(document.key, Schema.STRING_SCHEMA, document.json, offset);
    }

    static SinkRecord struct(Document document, long offset) {
        Struct value = new Struct(VALUE)
                .put("title", document.title)
                .put("description", document.description)
                .put("brand", document.brand)
                .put("tags", document.tags)
                .put("price", document.price)
                .put("views", document.views)
                .put("locked", document.locked);
        return record(document.key, VALUE, value, offset);
    }

    static SinkRecord rawPut(Document document, long offset) {
        String put = "{\"put\":\"id:perf:item::" + document.key + "\",\"fields\":" + document.json + "}";
        return record(null, Schema.STRING_SCHEMA, put, offset);
    }

    private static SinkRecord record(String key, Schema valueSchema, Object value, long offset) {
        return new SinkRecord(TOPIC, (int) (offset % PARTITIONS), Schema.STRING_SCHEMA, key, valueSchema, value, offset);
    }

    private static String words(Random random, int count) {
        return IntStream.range(0, count).mapToObj(i -> WORDS[random.nextInt(WORDS.length)]).collect(Collectors.joining(" "));
    }
}
