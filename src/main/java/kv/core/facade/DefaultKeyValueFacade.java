package kv.core.facade;

import kv.core.StorageEngine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class DefaultKeyValueFacade implements KeyValueApi {
    private final StorageEngine storageEngine;

    public DefaultKeyValueFacade(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    public void put(String key, String value) throws IOException {
        storageEngine.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String read(String key) throws IOException {
        byte[] value = storageEngine.read(key.getBytes(StandardCharsets.UTF_8));
        if (value == null) {
            return null;
        }
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public Map<String, String> readKeyRange(String startKey, String endKey) throws IOException {
       Map<byte[], byte[]> values = storageEngine.readRange(startKey.getBytes(StandardCharsets.UTF_8), endKey.getBytes(StandardCharsets.UTF_8));

       Map<String, String> result = new HashMap<>();
       for (Map.Entry<byte[], byte[]> entry : values.entrySet()) {
           String key = new String(entry.getKey(), StandardCharsets.UTF_8);
           byte[] value = entry.getValue();
           String valueString = value == null ? null : new String(value, StandardCharsets.UTF_8);
           result.put(key, valueString);
       }

       return result;
    }

    @Override
    public void batchPut(Map<String, String> entries) throws IOException {
        if (entries == null || entries.isEmpty()) return;
        Map<byte[], byte[]> values = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) continue;
            values.put(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue().getBytes(StandardCharsets.UTF_8));
        }
        storageEngine.batchPut(values);
    }

    @Override
    public void delete(String key) throws IOException {
        if (key == null) return;
        storageEngine.delete(key.getBytes(StandardCharsets.UTF_8));
    }
}
