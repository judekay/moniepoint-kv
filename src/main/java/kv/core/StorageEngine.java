package kv.core;

import java.io.IOException;
import java.util.Map;

public interface StorageEngine extends AutoCloseable {
    void put(byte[] key, byte[] value) throws IOException;

    byte[] read(byte[] key) throws IOException;

    Map<byte[], byte[]> readRange(byte[] startKey, byte[] endKey) throws IOException;

    void batchPut(Map<byte[], byte[]> entries) throws IOException;

    void delete(byte[] key) throws IOException;
}
