package kv.core;

import kv.storage.SsTableHandler;
import kv.storage.SsTableWriter;
import kv.storage.WriteAheadLog;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
        writeAheadLog = new WriteAheadLog(new File(dataFile, "writeAheadLog.log"));
    }
    @Override
    public void put(byte[] key, byte[] value) throws IOException {
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

    public void close() throws IOException {
        readWriteLock.writeLock().lock();
        try{
            writeAheadLog.close();
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void flushMemTableToSsTable() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }

        File sstableFile = ssTableHandler.newSsTableFile();
        try (SsTableWriter ssTableWriter = new SsTableWriter(sstableFile)) {
            ssTableWriter.writeFromMemTable(memTable);
        }

        ssTableHandler.registerSsTable(sstableFile);
        memTable.clear();
    }
}
