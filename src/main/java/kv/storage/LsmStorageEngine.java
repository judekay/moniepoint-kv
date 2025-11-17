package kv.storage;

import kv.core.StorageEngine;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmStorageEngine implements StorageEngine {
    private final MemTable memTable;
    private final WriteAheadLog writeAheadLog;
    private final ReentrantReadWriteLock readWriteLock= new ReentrantReadWriteLock(true);
    private final SsTableHandler ssTableHandler;

    private final List<File> sstableFiles = new ArrayList<>();
    private final File dataFile;

    private final int memtableMaxLimit = 1000;

    public LsmStorageEngine(File dataFile) throws IOException {
        this.dataFile = dataFile;
        if (!dataFile.exists() && !dataFile.mkdirs()) {
            throw new IOException("File " + dataFile.getAbsolutePath() + " does not exist");
        }
        this.memTable = new MemTable();
        this.ssTableHandler = new SsTableHandler(dataFile);
        this.writeAheadLog = new WriteAheadLog(new File(dataFile, "writeAheadLog.log"));

        replayWriteAheadLogIntoMemTable();
    }
    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");

        String keyString = new String(key, StandardCharsets.UTF_8);
        readWriteLock.writeLock().lock();

        try{
            //first append to write ahead log
            writeAheadLog.appendPut(key, value);
            //then put to memtable
            memTable.put(keyString, value);
            //flush if the threshold is exceeded
            if (memTable.size() >= memtableMaxLimit) {
                //flush memtable to sstable
                flushMemTableToSsTable();
                //todo add logging in here
                System.out.printf("Memtable size (%.2f kb) exceeded limit (%.2f kb)\n", memTable.getSizeInBytes() / 1024.0, memtableMaxLimit / 1024.0);
                writeAheadLog.reset();
            }

        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public byte[] read(byte[] keyInBytes) throws IOException {
        Objects.requireNonNull(keyInBytes, "key must not be null");
        String key = new String(keyInBytes, StandardCharsets.UTF_8);

        readWriteLock.readLock().lock();
        try {
            //read from Memtable first
            Entry inMemTable = memTable.get(key);
            if (inMemTable != null) {
                return inMemTable.deleted() ? null : inMemTable.value();
            }

            // then from SStables
            Entry inSsTable = ssTableHandler.get(key);
            if (inSsTable == null || inSsTable.deleted()) {
                return null;
            }
            return inSsTable.value();
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public Map<byte[], byte[]> readRange(byte[] startKeyInBytes, byte[] endKeyInBytes) throws IOException {
        Objects.requireNonNull(startKeyInBytes, "startKey must not be null");
        Objects.requireNonNull(endKeyInBytes, "endKey must not be null");

        String startKey = new String(startKeyInBytes, StandardCharsets.UTF_8);
        String endKey = new String(endKeyInBytes, StandardCharsets.UTF_8);

        readWriteLock.readLock().lock();
        try {
            NavigableMap<String, Entry> merged = new TreeMap<>();

            NavigableMap<String, Entry> fromSsTable = ssTableHandler.getRange(startKey, endKey);
            merged.putAll(fromSsTable);

            // overwrite older entries
            merged.putAll(memTable.readKeyRange(startKey, endKey));

            Map<byte[], byte[]> result = new LinkedHashMap<>();
            for (Map.Entry<String, Entry> entry : merged.entrySet()) {
                Entry value = entry.getValue();
                if (value.deleted()) continue;
                result.put(
                        entry.getKey().getBytes(StandardCharsets.UTF_8),
                        value.value()
                );
            }

            return result;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void batchPut(Map<byte[], byte[]> entries) throws IOException {
        if (entries == null || entries.isEmpty()) return;

        for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            if  (key == null || value == null) continue;

            put(key, value);
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        Objects.requireNonNull(key, "key must not be null");
        String keyString = new String(key, StandardCharsets.UTF_8);

        readWriteLock.writeLock().lock();
        try{
            writeAheadLog.appendDelete(key);
            memTable.delete(keyString);

            if (memTable.size() >= memtableMaxLimit) {
                flushMemTableToSsTable();
                writeAheadLog.reset();
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public void close() throws IOException {
        readWriteLock.writeLock().lock();
        try{
            writeAheadLog.close();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void flushMemTableToSsTable() throws IOException {
        if (memTable.isEmpty()) return;

        File sstableFile = ssTableHandler.newSsTableFile();
        try (SsTableWriter ssTableWriter = new SsTableWriter(sstableFile)) {

            ssTableWriter.writeFromMemTable(memTable);
            ssTableHandler.registerSsTable(sstableFile, ssTableWriter.getOffsetIndex());
        }

        memTable.clear();

        ssTableHandler.compact();
    }

    private void replayWriteAheadLogIntoMemTable() throws IOException {
        readWriteLock.writeLock().lock();
        try {
            writeAheadLog.replay((outputByte, keyBytes, valueBytes) -> {
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                if (outputByte == WriteAheadLog.OP_PUT) {
                    memTable.put(key, valueBytes);
                } else if (outputByte == WriteAheadLog.OP_DELETE) {
                    memTable.delete(key);
                }

                //otherwise we ignore for now
            });
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
