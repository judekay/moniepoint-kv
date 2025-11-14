package kv.storage;

import kv.core.MemTable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SsTableWriter implements Closeable {

    private final File file;
    private final DataOutputStream out;

    public SsTableWriter(File file) throws IOException {
        this.file = file;
        this.out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
    }

    public File getFile() {
        return file;
    }

    public void write(String key, Entry entry) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = entry.value();

        out.writeInt(keyBytes.length);
        out.writeInt(valueBytes.length);
        out.write(keyBytes);
        out.write(valueBytes);
    }

    public void writeFromMemTable(MemTable memTable) throws IOException {
        for (Map.Entry<String, Entry> entry : memTable.getEntries()) {
            write(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void close() throws IOException {
        out.flush();
        out.close();
    }
}
