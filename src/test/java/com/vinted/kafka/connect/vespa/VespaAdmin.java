package com.vinted.kafka.connect.vespa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/** Counts and deletes the benchmark's documents through Vespa's query and document APIs. */
final class VespaAdmin {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String endpoint;
    private final HttpClient http = HttpClient.newHttpClient();

    VespaAdmin(String endpoint) {
        this.endpoint = endpoint.replaceAll("/+$", "");
    }

    long count() throws IOException, InterruptedException {
        String yql = URLEncoder.encode("select * from item where true", StandardCharsets.UTF_8);
        JsonNode response = send(HttpRequest.newBuilder(URI.create(endpoint + "/search/?hits=0&timeout=30s&yql=" + yql)).GET());
        return response.path("root").path("fields").path("totalCount").asLong();
    }

    /** How many documents have each value of {@code views}. */
    Map<Long, Long> countByViews() throws IOException, InterruptedException {
        String yql = URLEncoder.encode("select * from item where true limit 0 | all(group(views) max(1000) each(output(count())))",
                StandardCharsets.UTF_8);
        JsonNode response = send(HttpRequest.newBuilder(URI.create(endpoint + "/search/?timeout=30s&yql=" + yql)).GET());
        Map<Long, Long> counts = new TreeMap<>();
        for (JsonNode group : response.at("/root/children/0/children/0/children")) {
            counts.put(group.get("value").asLong(), group.at("/fields/count()").asLong());
        }
        return counts;
    }

    void deleteAll() throws IOException, InterruptedException {
        String continuation = "";
        do {
            String uri = endpoint + "/document/v1/perf/item/docid?selection=true&cluster=items&timeChunk=60s" + continuation;
            JsonNode response = send(HttpRequest.newBuilder(URI.create(uri)).DELETE());
            continuation = response.has("continuation")
                    ? "&continuation=" + URLEncoder.encode(response.get("continuation").asText(), StandardCharsets.UTF_8)
                    : "";
        } while (!continuation.isEmpty());
    }

    private JsonNode send(HttpRequest.Builder request) throws IOException, InterruptedException {
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException(response.statusCode() + " " + response.body());
        }
        return JSON.readTree(response.body());
    }
}
