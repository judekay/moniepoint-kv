package kv.api.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import kv.api.KeyValueServer;
import kv.core.facade.KeyValueApi;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class KeyValueHttpServer implements KeyValueServer {
    private final KeyValueApi facade;
    private final int port;
    private HttpServer server;

    public KeyValueHttpServer(KeyValueApi facade, int port) {
        this.facade = facade;
        this.port = port;
    }

    @Override
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/keyvalue", exchange -> {
            if ("PUT".equalsIgnoreCase(exchange.getRequestMethod())) {
                handlePut(exchange);
            } else if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                handleRead(exchange);
            } else if ("DELETE".equalsIgnoreCase(exchange.getRequestMethod())) {
                handleDelete(exchange);
            }
            else {
                sendResponse(exchange, 405, "Method not allowed");
            }
        });

        server.createContext("/keyvalue/range", this::handleReadKeyRange);
        server.createContext("/keyvalue/batch", this::handleBatchPut);
        server.setExecutor(null);
        server.start();
        System.out.println("KeyValueHttpServer started on port " + port);
    }

    @Override
    public void stop() {
        if (server != null) {
            System.out.println("Stopping KeyValueHttpServer");
            server.stop(0);
        }
    }

    private void handlePut(HttpExchange exchange) throws IOException {
        if (!"PUT".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method not allowed");
            return;
        }

        Map<String, String> query = parseQuery(exchange.getRequestURI());
        String key = query.get("key");
        if (key == null) {
            sendResponse(exchange, 400, "Missing key parameter");
            return;
        }

        String value = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        facade.put(key, value);
        sendResponse(exchange, 200, "OK");
    }

    private void handleRead(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method not allowed");
            return;
        }
        Map<String, String> query = parseQuery(exchange.getRequestURI());
        String key = query.get("key");

        if (key == null) {
            sendResponse(exchange, 400, "Missing key parameter");
            return;
        }

        String value = facade.read(key);
        if (value == null) {
            sendResponse(exchange, 404, "Not found");
        } else {
            sendResponse(exchange, 200, value);
        }
    }

    private void handleReadKeyRange(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method not allowed");
            return;
        }

        Map<String, String> query = parseQuery(exchange.getRequestURI());
        String startKey = query.get("startKey");
        String endKey = query.get("endKey");
        if (startKey == null || endKey == null) {
            sendResponse(exchange, 400, "Missing start or end key parameter");
            return;
        }

        Map<String, String> rangeResult = facade.readKeyRange(startKey, endKey);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{");
        stringBuilder.append("\"startKey\":\"").append(escapeJson(startKey)).append("\",");
        stringBuilder.append("\"endKey\":\"").append(escapeJson(endKey)).append("\",");
        stringBuilder.append("\"entries\":[");
        boolean first = true;
        for (Map.Entry<String, String> entry : rangeResult.entrySet()) {
            if (!first) {
                stringBuilder.append(",");
            }
            first = false;
            stringBuilder.append("{")
            .append("\"key\":\"").append(escapeJson(entry.getKey())).append("\",")
            .append("\"value\":\"").append(escapeJson(entry.getValue())).append("\"")
            .append("}");
        }
        stringBuilder.append("]}");

        byte[] body = stringBuilder.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        Map<String, String> query = parseQuery(exchange.getRequestURI());
        String key = query.get("key");

        if (key == null || key.isEmpty()) {
            sendResponse(exchange, 400, "Missing key parameter");
            return;
        }

        facade.delete(key);
        sendResponse(exchange, 200, "OK");
    }

    private void handleBatchPut(HttpExchange exchange) throws IOException {
        try {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

            Map<String, String> entries = getEntries(body);

            if (entries.isEmpty()) {
                sendResponse(exchange, 400, "No valid key=value pairs found in request body");
                return;
            }

            facade.batchPut(entries);

            sendResponse(exchange, 200, "OK");
        } catch (Exception e) {
            sendResponse(exchange, 500, "Internal Server Error: " + e.getMessage());
        }
    }

    private static Map<String, String> getEntries(String body) {
        Map<String, String> entries = new LinkedHashMap<>();

        String[] lines = body.split("\\r?\\n");
        for (String eachLine : lines) {
            if (eachLine.isBlank()) continue;
            int index = eachLine.indexOf('=');
            if (index <= 0) {
                //todo maybe return 400 since input is malformed
                continue;
            }
            String key = eachLine.substring(0, index);
            String value = eachLine.substring(index + 1);
            entries.put(key, value);
        }
        return entries;
    }

    private Map<String, String> parseQuery(URI uri) {
        Map<String, String> map = new HashMap<>();
        String query = uri.getQuery();
        if (query == null || query.isBlank()) return map;

        String[] pairs = query.split("&");
        for (String pair : pairs) {
            if (pair.isEmpty()) continue;
            String[] parts = pair.split("=", 2);
            map.put(parts[0], parts.length > 1 ? parts[1] : "");
        }

        return map;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
