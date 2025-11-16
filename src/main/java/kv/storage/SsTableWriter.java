package kv.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SsTableWriter implements Closeable {

    private final File file;
    private final DataOutputStream dataOutputStream;
    private final SsTableKeyOffsetIndex offsetIndex = new SsTableKeyOffsetIndex();

    private static final int INDEX_SPARSE_RATE = 128;
    private int counter = 0;

    public SsTableWriter(File file) throws IOException {
        this.file = file;
        this.dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
    }

    public File getFile() {
        return file;
    }

    public void write(String key, Entry entry) throws IOException {
        long offset = dataOutputStream.size();

        if (counter % INDEX_SPARSE_RATE == 0) {
            offsetIndex.add(key, offset);
        }
        counter++;

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = entry.value();

        dataOutputStream.writeInt(keyBytes.length);
        dataOutputStream.writeInt(valueBytes == null ? -1 : valueBytes.length);
        dataOutputStream.write(keyBytes);

        if (valueBytes != null) {
            dataOutputStream.write(valueBytes);
        }
    }

    public void writeFromMemTable(MemTable memTable) throws IOException {
        for (Map.Entry<String, Entry> entry : memTable.getEntries()) {
            write(entry.getKey(), entry.getValue());
        }
    }

    public SsTableKeyOffsetIndex getOffsetIndex() {
        return offsetIndex;
    }

    @Override
    public void close() throws IOException {
        dataOutputStream.flush();
        dataOutputStream.close();
    }
}
