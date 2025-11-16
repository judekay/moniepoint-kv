package kv.unit.core;

import kv.core.StorageEngine;
import kv.core.facade.DefaultKeyValueFacade;
import kv.core.facade.KeyValueApi;
import kv.storage.LsmStorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultKeyValueFacadeTest {

    private File tempDir;
    private StorageEngine storageEngine;
    private KeyValueApi api;

    private void setupFacade() throws Exception {
        tempDir = Files.createTempDirectory("keyvalue-facade-test").toFile();
        storageEngine = new LsmStorageEngine(tempDir);
        api = new DefaultKeyValueFacade(storageEngine);
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
        File[] kids = f.listFiles();
        if (kids != null) {
            for (File k : kids) {
                deleteRecursively(k);
            }
        }
        f.delete();
    }

    @Test
    void testPutAndGetFacade() throws Exception {
        setupFacade();

        api.put("test1", "value1");

        String value = api.read("test1");
        assertEquals("value1", value);
    }

    @Test
    void testGetKeyRangeFacade() throws Exception {
        setupFacade();

        api.put("a", "value1");
        api.put("b", "value2");
        api.put("c", "value3");

        Map<String, String> result = api.readKeyRange("a", "b");

        assertEquals(2, result.size());
        assertEquals("value1", result.get("a"));
        assertEquals("value2",result.get("b"));
        assertNull(result.get("c"));
    }

    @Test
    void putOverridesExistingValueThroughFacade() throws Exception {
        setupFacade();

        api.put("key1", "value1");
        api.put("key1", "value2");

        String value = api.read("key1");
        assertEquals("value2", value);
    }
}
