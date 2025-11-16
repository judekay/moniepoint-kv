package kv.replication;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class ReplicationClient {

    private final HttpClient httpClient;
    private final List<String> replicaBaseUrls;

    public ReplicationClient(List<String> replicaBaseUrls) {
        this.replicaBaseUrls = replicaBaseUrls;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(200))
                .build();
    }

    public void replicatePut(String key, String value) {
        if (replicaBaseUrls.isEmpty()) return;
        String encodedKey = urlEncode(key);
        for (String baseUrl : replicaBaseUrls) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/keyvalue?key=" + encodedKey))
                    .PUT(HttpRequest.BodyPublishers.ofString(value))
                    .build();
            sendRequest(req);
        }
    }

    public void replicateDelete(String key) {
        if (replicaBaseUrls.isEmpty()) return;
        String encodedKey = urlEncode(key);
        for (String base : replicaBaseUrls) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(base + "/keyvalue?key=" + encodedKey))
                    .DELETE()
                    .build();
            sendRequest(req);
        }
    }

    public void replicateBatchPut(Map<String, String> entries) {
        if (entries.isEmpty()) return;

        StringBuilder body = new StringBuilder();
        for (Map.Entry<String, String> e : entries.entrySet()) {
            body.append(e.getKey())
                    .append("=")
                    .append(e.getValue())
                    .append("\n");
        }

        for (String base : replicaBaseUrls) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(base + "/keyvalue/batch"))
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                    .build();
            sendRequest(req);
        }
    }

    private void sendRequest(HttpRequest req) {
        httpClient
                .sendAsync(req, HttpResponse.BodyHandlers.discarding())
                .whenComplete((resp, err) -> {
                    if (err != null) {
                        System.err.println("Replication Failed: " + err.getMessage());
                        //log replication error /retry
                    }
                });
    }

    private String urlEncode(String key) {
        return URLEncoder.encode(key, StandardCharsets.UTF_8);
    }
}
