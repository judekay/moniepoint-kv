package kv.integration.api.http;

import kv.api.http.KeyValueHttpServer;
import kv.api.KeyValueServer;
import kv.core.facade.DefaultKeyValueFacade;
import kv.core.facade.KeyValueApi;
import kv.core.StorageEngine;
import kv.storage.LsmStorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueHttpServerTest {

    private static final int PORT = 18080;

    private File tempDir;
    private StorageEngine storageEngine;
    private KeyValueServer server;

    private void setupServer() throws Exception {
        tempDir = Files.createTempDirectory("keyvalue-integration-data").toFile();
        storageEngine = new LsmStorageEngine(tempDir);
        KeyValueApi api = new DefaultKeyValueFacade(storageEngine);
        server = new KeyValueHttpServer(api, PORT);
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (storageEngine != null) {
            storageEngine.close();
        }
        if (tempDir != null && tempDir.exists()) {
            deleteRecursively(tempDir);
        }
    }

    private void deleteRecursively(File f) {
        if (f == null) return;
        File[] files = f.listFiles();
        if (files != null) {
            for (File k : files) {
                deleteRecursively(k);
            }
        }
        f.delete();
    }

    @Test
    void testPutAndGetOverHttp() throws Exception {
        setupServer();
        HttpClient client = HttpClient.newHttpClient();

        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue?key=test1"))
                .PUT(HttpRequest.BodyPublishers.ofString("value1"))
                .build();

        HttpResponse<String> putResponse =
                client.send(putReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, putResponse.statusCode());

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue?key=test1"))
                .GET()
                .build();

        HttpResponse<String> getResp =
                client.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, getResp.statusCode());
        assertEquals("value1", getResp.body());
    }

    @Test
    void testReadKeyRangeOverHttp() throws Exception {
        setupServer();
        HttpClient client = HttpClient.newHttpClient();

        sendPut(client, "a", "value1");
        sendPut(client, "b", "value2");
        sendPut(client, "c", "value3");

        HttpRequest rangeReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue/range?startKey=a&endKey=b"))
                .GET()
                .build();

        HttpResponse<String> rangeResp =
                client.send(rangeReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, rangeResp.statusCode());
        String body = rangeResp.body();

        assertTrue(body.contains("\"startKey\":\"a\""));
        assertTrue(body.contains("\"endKey\":\"b\""));
        assertTrue(body.contains("\"key\":\"a\""));
        assertTrue(body.contains("\"value\":\"value1\""));
        assertTrue(body.contains("\"key\":\"b\""));
        assertTrue(body.contains("\"value\":\"value2\""));

        assertFalse(body.contains("\"key\":\"c\""));
    }

    @Test
    void testGetNonExistingKeyReturns404() throws Exception {
        setupServer();
        HttpClient client = HttpClient.newHttpClient();

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue?key=test1"))
                .GET()
                .build();

        HttpResponse<String> resp =
                client.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, resp.statusCode());
    }

    @Test
    void testReadKeyRangeEmptyIntervalOverHttp() throws Exception {
        setupServer();
        HttpClient client = HttpClient.newHttpClient();

        sendPut(client, "a", "value1");
        sendPut(client, "b", "value2");

        HttpRequest rangeReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue/range?startKey=x&endKey=z"))
                .GET()
                .build();

        HttpResponse<String> resp =
                client.send(rangeReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, resp.statusCode());
        String body = resp.body();

        assertTrue(body.contains("\"entries\":[]"), "Expected empty entries array but got: " + body);
    }

    @Test
    void testBatchPutOverHttp() throws Exception {
        setupServer();
        HttpClient client = HttpClient.newHttpClient();

        String body = String.join("\n",
                "key1=value1",
                "key2=value2"
        );

        HttpRequest batchReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue/batch"))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> batchResp =
                client.send(batchReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, batchResp.statusCode(), "batchPut should return 200");

        HttpRequest getByKey1 = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue?key=key1"))
                .GET()
                .build();
        HttpResponse<String> key1Resp =
                client.send(getByKey1, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, key1Resp.statusCode());
        assertEquals("value1", key1Resp.body());

        HttpRequest getByKey2 = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue?key=key2"))
                .GET()
                .build();
        HttpResponse<String> key2Resp =
                client.send(getByKey2, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, key2Resp.statusCode());
        assertEquals("value2", key2Resp.body());
    }


    @Test
    void deleteRemovesKey() throws Exception {
        setupServer();
        HttpClient client = HttpClient.newHttpClient();

        sendPut(client, "key", "value");

        HttpRequest deleteReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue?key=key"))
                .DELETE()
                .build();

        HttpResponse<String> deleteResp =
                client.send(deleteReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, deleteResp.statusCode());

        HttpRequest getReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue?key=key"))
                .GET()
                .build();

        HttpResponse<String> getResp =
                client.send(getReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(404, getResp.statusCode());
    }



    private void sendPut(HttpClient client, String key, String value) throws Exception {
        HttpRequest putReq = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + PORT + "/keyvalue?key=" + key))
                .PUT(HttpRequest.BodyPublishers.ofString(value))
                .build();

        HttpResponse<String> resp =
                client.send(putReq, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, resp.statusCode());
    }
}
