package kv.storage;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class WriteAheadLog implements AutoCloseable{
    private final RandomAccessFile randomAccessFile;
    private static final int DEFAULT_SYNC_PERIOD = 100;
    private final int syncPeriod;
    private int pendingEntriesSinceLastSync = 0;

    public static final byte OP_PUT = 0x01;
    public static final byte OP_DELETE = 0x02;

    public WriteAheadLog(File writeAheadLogFile) throws IOException {
        this(writeAheadLogFile, DEFAULT_SYNC_PERIOD);
    }

    public WriteAheadLog(File writeAheadLogFile, int syncPeriod) throws IOException {
        this.randomAccessFile = new RandomAccessFile(writeAheadLogFile, "rw");
        this.randomAccessFile.seek(this.randomAccessFile.length());
        this.syncPeriod = Math.max(1, syncPeriod);
    }

    public void appendPut(byte[] key, byte[] value) throws IOException {
        randomAccessFile.seek(randomAccessFile.length());
        randomAccessFile.writeByte(OP_PUT);
        randomAccessFile.writeInt(key.length);
        randomAccessFile.writeInt(value.length);
        randomAccessFile.write(key);
        randomAccessFile.write(value);

        onEntryAppended();
    }

    public void appendDelete(byte[] key) throws IOException {
        randomAccessFile.seek(randomAccessFile.length());
        randomAccessFile.writeByte(OP_DELETE);
        randomAccessFile.writeInt(key.length);
        //set to zero since no value bytes for delete
        randomAccessFile.writeInt(0);
        randomAccessFile.write(key);

        onEntryAppended();
    }

    public void replay(WriteAheadLogReplayHandler handler) throws IOException {
        long originalPos = randomAccessFile.getFilePointer();
        randomAccessFile.seek(0);

        try {
            while (true) {
                byte outputByte;
                try {
                    outputByte = randomAccessFile.readByte();
                } catch (EOFException eof) {
                    break;
                }

                int keyLength = randomAccessFile.readInt();
                int valueLength = randomAccessFile.readInt();

                if (keyLength < 0) {
                    throw new IOException("Invalid write ahead log");
                }

                byte[] key = new byte[keyLength];
                randomAccessFile.readFully(key);

                byte[] value = null;
                if (outputByte == OP_PUT) {
                    if (valueLength < 0) {
                        throw new IOException("Invalid write ahead log valueLength for PUT");
                    }
                    value = new byte[valueLength];
                    randomAccessFile.readFully(value);
                } else if (outputByte == OP_DELETE) {
                    if (valueLength > 0) {
                        randomAccessFile.skipBytes(valueLength);
                    }
                } else {
                    throw new IOException("Invalid write ahead unknown outputByte" + outputByte);
                }

                handler.onEntry(outputByte, key, value);
            }
        } finally {
            randomAccessFile.seek(originalPos);
        }
    }

    public void reset() throws IOException {
        forceSync();
        randomAccessFile.setLength(0);
        randomAccessFile.seek(0);
    }

    @Override
    public void close() throws IOException {
        try {
            forceSync();
        } finally {
            randomAccessFile.close();
        }
    }

    @FunctionalInterface
    public interface WriteAheadLogReplayHandler {
        void onEntry(byte outputByte, byte[] key, byte[] value) throws IOException;
    }

    protected void doSync() throws IOException {
        randomAccessFile.getFD().sync();
    }

    private void onEntryAppended() throws IOException {
        pendingEntriesSinceLastSync++;
        if (pendingEntriesSinceLastSync >= syncPeriod) {
            doSync();
            pendingEntriesSinceLastSync = 0;
        }
    }

    public void forceSync() throws IOException {
        if (pendingEntriesSinceLastSync > 0) {
            doSync();
            pendingEntriesSinceLastSync = 0;
        }
    }
}
