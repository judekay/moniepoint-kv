package kv.unit.storage;

import kv.storage.LsmStorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import kv.core.StorageEngine;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class LsmStorageEngineTest {

    private File tempDir;
    private StorageEngine storageEngine;

    private void setupEngine() throws Exception {
        tempDir = Files.createTempDirectory("keyvalue-engine-test").toFile();
        storageEngine = new LsmStorageEngine(tempDir);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (storageEngine != null) {
            storageEngine.close();
        }
        if (tempDir != null && tempDir.exists()) {
            deleteRecursively(tempDir);
        }
    }

    private void deleteRecursively(File f) {
        File[] children = f.listFiles();
        if (children != null) {
            for (File c : children) {
                deleteRecursively(c);
            }
        }
        f.delete();
    }

    @Test
    void testSingleKeyPutAndRead() throws Exception {
        setupEngine();

        storageEngine.put("testKey1".getBytes(), "testValue1".getBytes());

        byte[] value = storageEngine.read("testKey1".getBytes());

        assertNotNull(value);
        assertEquals("testValue1", new String(value));
    }

    @Test
    void readUnknownKeyReturnsNull() throws Exception {
        setupEngine();
        byte[] value = storageEngine.read("unknown".getBytes());
        assertNull(value);
    }

    @Test
    void rangeReturnsKeysWithinInclusiveBounds() throws Exception {
        setupEngine();

        storageEngine.put("test1".getBytes(), "value1".getBytes());
        storageEngine.put("test2".getBytes(), "value2".getBytes());

        Map<byte[], byte[]> result = storageEngine.readRange("test1".getBytes(), "test2".getBytes());

        assertEquals(2, result.size());

        boolean hasTest1 = false;
        boolean hasTest2 = false;

        for (Map.Entry<byte[], byte[]> entry : result.entrySet()) {
            String key = new String(entry.getKey(), StandardCharsets.UTF_8);
            String value = new String(entry.getValue(), StandardCharsets.UTF_8);
            if (key.equals("test1")) {
                hasTest1 = true;
                assertEquals("value1", value);
            } else if (key.equals("test2")) {
                hasTest2 = true;
                assertEquals("value2", value);
            }
        }

        assertTrue(hasTest1);
        assertTrue(hasTest2);
    }

    @Test
    void rangeReturnsEmptyWhenWhenNoKeysInInterval() throws Exception {
        setupEngine();

        storageEngine.put("test1".getBytes(), "value1".getBytes());
        storageEngine.put("test2".getBytes(), "value2".getBytes());

        Map<byte[], byte[]> result = storageEngine.readRange("test3".getBytes(), "value3".getBytes());

        assertTrue(result.isEmpty());
    }

    @Test
    void putOverridesExistingValue() throws Exception {
        setupEngine();

        storageEngine.put("key1".getBytes(), "value1".getBytes());
        storageEngine.put("key1".getBytes(), "value2".getBytes());

        byte[] value = storageEngine.read("key1".getBytes());
        assertNotNull(value);
        assertEquals("value2", new String(value));
    }

    @Test
    void rangeWithSingleKey() throws Exception {
        setupEngine();

        storageEngine.put("a".getBytes(), "value1".getBytes());
        storageEngine.put("b".getBytes(), "value2".getBytes());
        storageEngine.put("c".getBytes(), "value3".getBytes());

        Map<byte[], byte[]> result =
                storageEngine.readRange("b".getBytes(), "b".getBytes());

        assertEquals(1, result.size());

        Map.Entry<byte[], byte[]> entry =
                result.entrySet().iterator().next();

        assertEquals("b", new String(entry.getKey()));
        assertEquals("value2", new String(entry.getValue()));
    }

    @Test
    void testBatchPutWithMultipleKeysAndValues() throws Exception {
        setupEngine();

        Map<byte[], byte[]> entries = new LinkedHashMap<>();
        entries.put("key1".getBytes(), "value1".getBytes());
        entries.put("key2".getBytes(), "value2".getBytes());

        storageEngine.batchPut(entries);

        assertEquals("value1", new String(storageEngine.read("key1".getBytes())));
        assertEquals("value2", new String(storageEngine.read("key2".getBytes())));
    }

    @Test
    void deleteRemovesKeyFromGet() throws Exception {
        setupEngine();

        storageEngine.put("key".getBytes(), "value".getBytes());
        assertNotNull(storageEngine.read("key".getBytes()));

        storageEngine.delete("key".getBytes());
        assertNull(storageEngine.read("key".getBytes()));
    }

    @Test
    void deleteRemovesKeyFromRange() throws Exception {
        setupEngine();

        storageEngine.put("a".getBytes(), "value1".getBytes());
        storageEngine.put("b".getBytes(), "value2".getBytes());
        storageEngine.put("c".getBytes(), "value3".getBytes());

        storageEngine.delete("b".getBytes());

        Map<byte[], byte[]> result =
                storageEngine.readRange("a".getBytes(), "c".getBytes());

        Set<String> keys = new LinkedHashSet<>();
        for (Map.Entry<byte[], byte[]> entry : result.entrySet()) {
            keys.add(new String(entry.getKey()));
        }

        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("c"));
        assertFalse(keys.contains("b"));
    }

    @Test
    void testWriteAheadReplayRestoresMemTableOnRestart() throws Exception {
        File dir = Files.createTempDirectory("keyvalue-writeAhead-replay-test").toFile();

        try {
            LsmStorageEngine storageEngine1 = new LsmStorageEngine(dir);

            storageEngine1.put("key1".getBytes(StandardCharsets.UTF_8),
                    "value1".getBytes(StandardCharsets.UTF_8));

            storageEngine1.put("key2".getBytes(StandardCharsets.UTF_8),
                    "value2".getBytes(StandardCharsets.UTF_8));
            storageEngine1.delete("key2".getBytes(StandardCharsets.UTF_8));

            storageEngine1.close();

            LsmStorageEngine storageEngine2 = new LsmStorageEngine(dir);

            byte[] firstEntry = storageEngine2.read("key1".getBytes(StandardCharsets.UTF_8));
            byte[] deletedEntry = storageEngine2.read("key2".getBytes(StandardCharsets.UTF_8));

            assertNotNull(firstEntry, "first key should be restored from Write ahead log");
            assertEquals("value1", new String(firstEntry, StandardCharsets.UTF_8));

            assertNull(deletedEntry, "key2 should still be deleted after write ahead log replay");

            storageEngine2.close();
        } finally {
            deleteRecursively(dir);
        }
    }
}
