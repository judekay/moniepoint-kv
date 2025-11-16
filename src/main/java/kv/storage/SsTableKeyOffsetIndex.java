package kv.storage;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class SsTableKeyOffsetIndex {

    private final NavigableMap<String, Long> storageMapIndex = new TreeMap<>();

    void add(String key, long offset) {
        storageMapIndex.put(key, offset);
    }

    Map.Entry<String,Long> get(String key) {
        return storageMapIndex.floorEntry(key);
    }

    boolean isEmpty() {
        return storageMapIndex.isEmpty();
    }
}
