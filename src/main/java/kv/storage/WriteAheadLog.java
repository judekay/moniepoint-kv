package kv.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class WriteAheadLog implements AutoCloseable{
    private final RandomAccessFile randomAccessFile;

    public WriteAheadLog(File writeAheadLogFile) throws IOException {
        this.randomAccessFile = new RandomAccessFile(writeAheadLogFile, "rw");
        this.randomAccessFile.seek(this.randomAccessFile.length());
    }

    public void appendPut(byte[] key, byte[] value) throws IOException {
        randomAccessFile.seek(randomAccessFile.length());
        randomAccessFile.writeByte(0x01);
        randomAccessFile.writeInt(key.length);
        randomAccessFile.writeInt(value.length);
        randomAccessFile.write(key);
        randomAccessFile.write(value);

        //todo we might need to batch this later for performance
        randomAccessFile.getFD().sync();
    }

    public void reset() throws IOException {
        randomAccessFile.setLength(0);
        randomAccessFile.seek(0);
    }

    @Override
    public void close() throws IOException {
        randomAccessFile.close();
    }
}
