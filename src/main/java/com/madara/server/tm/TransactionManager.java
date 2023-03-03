package com.madara.server.tm;

import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.server.tm.impl.TransactionManagerImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.madara.common.Constants.XID_FILE_SUFFIX;
import static com.madara.common.Constants.XID_HEADER_LENGTH;

public interface TransactionManager {
    public long begin();//开启新事务

    public void commit(long xid);//提交事务

    public void abort(long xid);//取消事务

    public boolean isActive(long xid);//事务是否进行态

    public boolean isCommitted(long xid);//事务是否已提交

    public boolean isAbort(long xid);//事务是否已取消

    public void close();//关闭事务

    public static TransactionManager create(String path) {
        File file = new File(path + XID_FILE_SUFFIX);
        try {
            if (!file.createNewFile()) {
                throw Error.FileExistsException;
            }
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        RandomAccessFile frw = null;
        FileChannel fc = null;
        try {
             frw = new RandomAccessFile(file, "rw");
             fc = frw.getChannel();
        } catch (Exception e) {
            Exit.systemExit(e);
        }
//        占文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            while (buf.hasRemaining()) {
                fc.write(buf);
            }
        } catch (IOException e) {
            Exit.systemExit(e);
        }
        return new TransactionManagerImpl(frw,fc);
    }

    public static TransactionManager open(String path) {
        File f = new File(path+XID_FILE_SUFFIX);
        if(!f.exists()) {
            Exit.systemExit(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Exit.systemExit(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile frw = null;
        try {
            frw = new RandomAccessFile(f, "rw");
            fc = frw.getChannel();
        } catch (FileNotFoundException e) {
            Exit.systemExit(e);
        }
        return new TransactionManagerImpl(frw, fc);
    }
}
