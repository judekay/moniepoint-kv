package kv.core.facade;

import kv.core.StorageEngine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class DefaultKeyValueFacade implements KeyValueApi {
    private final StorageEngine storageEngine;

    public DefaultKeyValueFacade(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    public void put(String key, String value) throws IOException {
        storageEngine.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }
}
