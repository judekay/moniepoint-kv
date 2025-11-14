package kv.core;

import java.io.IOException;
import java.util.Map;

public interface StorageEngine extends AutoCloseable {
    void put(byte[] key, byte[] value) throws IOException;

    default byte[] read(byte[] key) throws IOException {
        throw  new UnsupportedOperationException("Get operation not supported yet");
    }

    default Map<byte[], byte[]> readRange(byte[] startKey, byte[] endKey) throws IOException {
        throw  new UnsupportedOperationException("ReadRange operation not supported yet");
    }

    default void batchPut(Map<byte[], byte[]> values) throws IOException {
        throw  new UnsupportedOperationException("BatchPut operation not supported yet");
    }

    default void delete(byte[] key) throws IOException {
        throw  new UnsupportedOperationException("Delete operation not supported yet");
    }
}
