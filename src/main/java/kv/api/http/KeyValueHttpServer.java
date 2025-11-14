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
import java.util.HashMap;
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

        //for put
        server.createContext("/keyValue", this::handlePut);
        server.setExecutor(null);
        server.start();
        System.out.println("KeyValueHttpServer started on port " + port);
    }

    @Override
    public void stop() {

    }

    private void handlePut(HttpExchange exchange) throws IOException {
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
}
