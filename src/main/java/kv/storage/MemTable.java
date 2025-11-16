package kv.storage;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable {

    private final NavigableMap<String, Entry> storageMap = new TreeMap<>();
    private volatile long sizeInBytes = 0;

    public void put(String key, byte[] value)  {
        long entrySizeInBytes = key.length() + (value != null ? value.length : 0) + 32;
        storageMap.put(key, new Entry(value));
        sizeInBytes += entrySizeInBytes;
    }

    public Entry get(String key) {
        return storageMap.get(key);
    }

    public NavigableMap<String, Entry> readKeyRange(String startKey, String endKey) {
        return storageMap.subMap(startKey, true, endKey, true);
    }

    public int size() {
        return storageMap.size();
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public boolean isEmpty() {
        return storageMap.isEmpty();
    }

    public void clear() {
        storageMap.clear();
    }

    public Iterable<Map.Entry<String, Entry>> getEntries() {
        return storageMap.entrySet();
    }
}
