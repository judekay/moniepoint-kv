package kv.storage;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SsTableHandler {

    private final File file;
    private final List<File> sstableFiles = new ArrayList<>();
    private final Map<File, SsTableKeyOffsetIndex> storageMapIndex = new HashMap<>();

    public SsTableHandler(File file) throws IOException {
        this.file = file;
        loadExistingSsTable();
    }

    private void loadExistingSsTable() throws IOException {
        File[] files = file.listFiles(new  FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("sstable_") && name.endsWith(".dat");
            }
        });

        if (files != null) {
            Arrays.sort(files);
            for (File file : files) {
                sstableFiles.add(file);
                storageMapIndex.put(file, buildIndexForExistingFile(file));
            }
        }
    }

    public File newSsTableFile() {
        return new File(file.getParentFile(), file.getName() + ".sstable");
    }

    public List<File> getSsTableFile() {
        return Collections.unmodifiableList(sstableFiles);
    }

    public void registerSsTable(File file, SsTableKeyOffsetIndex ssTableKeyOffsetIndex) {
        sstableFiles.add(file);
        storageMapIndex.put(file, ssTableKeyOffsetIndex);
    }

    public Entry get(String key)  throws IOException {
        for (int i = sstableFiles.size() - 1; i >= 0; i--) {
            File file = sstableFiles.get(i);
            SsTableKeyOffsetIndex offsetIndex = storageMapIndex.get(file);
            Entry entry = getFromFile(file, offsetIndex, key);
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    public NavigableMap<String, Entry> getRange(String startKey, String endKey)  throws IOException {
        NavigableMap<String, Entry> ranges = new TreeMap<>();
        for (int i = sstableFiles.size() - 1; i >= 0; i--) {
            File file = sstableFiles.get(i);
            SsTableKeyOffsetIndex offsetIndex = storageMapIndex.get(file);

            NavigableMap<String, Entry> partialRange = rangeFromFile(file, offsetIndex, startKey, endKey);

            ranges.putAll(partialRange);
        }
        return ranges;
    }

    private SsTableKeyOffsetIndex buildIndexForExistingFile(File file) throws IOException {
        SsTableKeyOffsetIndex offsetIndex = new SsTableKeyOffsetIndex();
        final int INDEX_SPARSE_RATE = 128;
        int counter = 0;

        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            while (true) {
                long offset = randomAccessFile.getFilePointer();
                try {
                    int keyLength = randomAccessFile.readInt();
                    int valueLength = randomAccessFile.readInt();

                    byte[] keyBytes = new byte[keyLength];
                    randomAccessFile.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    if (valueLength > 0) {
                        randomAccessFile.skipBytes(valueLength);
                    }

                    if (counter % INDEX_SPARSE_RATE == 0) {
                        offsetIndex.add(key, offset);
                    }
                    counter++;
                } catch (EOFException e) {
                    break;
                }
            }
        }
        return offsetIndex;
    }

    private Entry getFromFile(File file, SsTableKeyOffsetIndex offsetIndex, String key) throws IOException {
        long startOffset = 0L;

        if (offsetIndex != null && !offsetIndex.isEmpty()) {
            Map.Entry<String, Long> floorEntry = offsetIndex.get(key);
            if (floorEntry != null) {
                startOffset = floorEntry.getValue();
            }
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(startOffset);

            while (true) {
                try {
                    int keyLength = randomAccessFile.readInt();
                    int valueLength = randomAccessFile.readInt();

                    byte[] keyBytes = new byte[keyLength];
                    randomAccessFile.readFully(keyBytes);

                    String entryKey = new String(keyBytes, StandardCharsets.UTF_8);

                    if (valueLength <  0) {
                        throw new IOException("Invalid key length: " + valueLength);
                    }
                    byte[] valueBytes = new byte[valueLength];
                    randomAccessFile.readFully(valueBytes);

                    int keyCompare  = entryKey.compareTo(key);
                    if (keyCompare == 0) {
                        return new Entry(valueBytes, false);
                    } else if (keyCompare > 0) {
                        break;
                    }
                } catch (EOFException e) {
                    break;
                }
            }

        }
        return null;
    }

    private NavigableMap<String, Entry> rangeFromFile(File file, SsTableKeyOffsetIndex offsetIndex, String startKey, String endKey) throws IOException {
        NavigableMap<String, Entry> ranges = new TreeMap<>();

        long startOffset = 0L;
        if (offsetIndex != null && offsetIndex.isEmpty()) {
            Map.Entry<String, Long> floorEntry = offsetIndex.get(startKey);
            if (floorEntry != null) {
                startOffset = floorEntry.getValue();
            }
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(startOffset);

            while (true) {
                try {
                    int keyLength = randomAccessFile.readInt();
                    int valueLength = randomAccessFile.readInt();

                    byte[] keyBytes = new byte[keyLength];
                    randomAccessFile.readFully(keyBytes);

                    String entryKey = new String(keyBytes, StandardCharsets.UTF_8);

                    if (valueLength <  0) {
                        throw new IOException("Invalid key length: " + valueLength);
                    }
                    byte[] valueBytes = new byte[valueLength];
                    randomAccessFile.readFully(valueBytes);

                    if (entryKey.compareTo(endKey) > 0) {
                        break;
                    }

                    if (entryKey.compareTo(startKey) >= 0) {
                        ranges.put(entryKey, new Entry(valueBytes, false));
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        }
        return ranges;
    }
}
