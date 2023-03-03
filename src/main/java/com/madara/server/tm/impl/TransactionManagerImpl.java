package com.madara.server.tm.impl;

import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.server.tm.TransactionManager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

import static com.madara.common.Constants.*;

/**
 * @author Madara
 */
public class TransactionManagerImpl implements TransactionManager {
    private RandomAccessFile file;
    private FileChannel fc;
    private ReentrantLock lock = new ReentrantLock();
    private long XIDCounter;
    public TransactionManagerImpl(RandomAccessFile frw, FileChannel fc) {
        this.file = frw;
        this.fc = fc;
        checkXIDFile();
    }
    private void checkXIDFile() {
        long fileLen = 0;
        try {
            if ((fileLen = file.length()) < XID_HEADER_LENGTH) {
                Exit.systemExit(Error.BadXIDFileException);
            }
            long xidHeader =0;
            fc.position(0);
            ByteBuffer buf = ByteBuffer.allocate(XID_HEADER_LENGTH);
            fc.read(buf);
            buf.flip();
            xidHeader = buf.getLong();
            this.XIDCounter = xidHeader;
            if (fileLen != getXIDPosition(xidHeader)+1) {
                throw new IOException();
            }
        } catch (IOException e) {
            Exit.systemExit(Error.BadXIDFileException);
        }
    }

    private long getXIDPosition(long xid) {
        return XID_HEADER_LENGTH + xid-1;
    }
    private void updateXID(long xid, byte status) {
        long xidPosition = getXIDPosition(xid);
        byte[] bytes = new byte[XID_FIELD_LENGTH];
        bytes[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        try {
            fc.position(xidPosition);
            fc.write(buf);
            fc.force(false);
        } catch (Exception e) {
            Exit.systemExit(e);
        }
    }
    @Override
    public long begin() {
        lock.lock();
        try {
            long xid = ++XIDCounter;
            updateXID(xid,XID_ACTIVE_STATUS);
            updateXIDFileHeader();
            return xid;
        }finally {
            lock.unlock();
        }
    }
    private boolean checkXIDStatus(long xid,byte status) {
        byte b = 0;
        try {
            long xidPosition = getXIDPosition(xid);
            fc.position(xidPosition);
            ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_LENGTH]);
            fc.read(buf);
            buf.flip();
            b = buf.get();
        } catch (IOException e) {
            Exit.systemExit(e);
        }
        return b == status;
    }
    private void updateXIDFileHeader() {
        try {
            byte[] bytes = new byte[XID_HEADER_LENGTH];
            ByteBuffer buf = ByteBuffer.wrap(bytes).putLong(XIDCounter);
            fc.position(0);
            buf.clear();
            fc.write(buf);
        } catch (IOException e) {
            Exit.systemExit(e);
        }
    }
    @Override
    public void commit(long xid) {
        updateXID(xid,XID_COMMITED_STATUS);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid,XID_ABORT_STATUS);
    }

    @Override
    public boolean isActive(long xid) {
        return checkXIDStatus(xid, XID_ACTIVE_STATUS);
    }

    @Override
    public boolean isCommitted(long xid) {
        return checkXIDStatus(xid, XID_COMMITED_STATUS);
    }

    @Override
    public boolean isAbort(long xid) {
        return checkXIDStatus(xid, XID_ABORT_STATUS);
    }

    @Override
    public void close() {
        try {
            file.close();
            fc.close();
        } catch (IOException e) {
            Exit.systemExit(e);
        }
    }
}
