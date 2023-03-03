package com.madara.server.dm.logger;

import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.server.dm.logger.impl.LoggerImpl;
import com.madara.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.madara.common.Constants.*;

public interface Logger {
    void log(byte[] data);

    void truncate(long x);

    byte[] next();

    void rewind();

    void close();

    public static LoggerImpl create(String path) {
        File f = new File(path+LOG_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Exit.systemExit(Error.FileExistsException);
            }
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Exit.systemExit(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Exit.systemExit(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Exit.systemExit(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }
    public static LoggerImpl open(String path) {
        File f = new File(path+LOG_SUFFIX);
        if(!f.exists()) {
            Exit.systemExit(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Exit.systemExit(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Exit.systemExit(e);
        }

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();
        return lg;
    }
}
