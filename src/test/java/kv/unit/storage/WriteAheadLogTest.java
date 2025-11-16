package kv.unit.storage;

import kv.storage.WriteAheadLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WriteAheadLogTest {

    private File tempFile;

    //intercept sync calls
    static class TestWriteAheadLog extends WriteAheadLog {
        int syncCount = 0;

        public TestWriteAheadLog(File file, int syncPeriod) throws IOException {
            super(file, syncPeriod);
        }

        @Override
        protected void doSync() {
            syncCount++;
        }
    }

    @AfterEach
    void cleanup() throws Exception {
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    void syncIsBatchedBasedOnSyncPeriod() throws Exception {
        tempFile = Files.createTempFile("writeAheadLog-batch-test", ".log").toFile();

        TestWriteAheadLog writeAheadLog = new TestWriteAheadLog(tempFile, 3);

        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        writeAheadLog.appendPut(key, value);
        writeAheadLog.appendPut(key, value);
        // triggers 1st sync on 3rd append
        writeAheadLog.appendPut(key, value);

        assertEquals(1, writeAheadLog.syncCount, "Expected 1 sync after 3 appends");

        writeAheadLog.appendPut(key, value);
        writeAheadLog.appendPut(key, value);
        // triggers 2nd on 6th append
        writeAheadLog.appendPut(key, value);

        assertEquals(2, writeAheadLog.syncCount, "Expected 2 syncs after 6 appends");

        writeAheadLog.appendPut(key, value);
        writeAheadLog.forceSync();

        assertEquals(3, writeAheadLog.syncCount, "Expected forceSync to trigger an extra sync");

        writeAheadLog.close();
    }
}
