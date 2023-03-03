package com.madara.server.dm.logger.impl;

import com.google.common.primitives.Bytes;
import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.utils.Parser;
import com.madara.server.dm.logger.Logger;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static com.madara.common.Constants.*;
/**
 * @author Madara
 *
 * 日志文件读写
 *
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 *
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;
    private long fileSize;
    private int xCheckSum;
//  逐条解析日志的指针
    private long logPosition;
    public LoggerImpl(RandomAccessFile file, FileChannel fc, int xCheckSum) {
        this.file = file;
        this.fc = fc;
        this.xCheckSum = xCheckSum;
        lock = new ReentrantLock();
    }
    public LoggerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        lock = new ReentrantLock();
    }
//重新打开日志文件时初始化日志尾指针，检查总校验和，检查BadTail 并将其移除
    public void init() {
        long size = 0;
        try {
            size = file.length();
            if (size < LOG_CHECK_SUM_OFFSET) {
                throw Error.BadLogFileException;
            }
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Exit.systemExit(e);
        }
        int xCheckSum = Parser.byte2Int(buf.array());
        this.fileSize = size;
        this.xCheckSum = xCheckSum;
        checkAndRemoveTail();
    }
    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xCheckSum) {
            Exit.systemExit(Error.BadLogFileException);
        }

        try {
            truncate(logPosition);
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        try {
            file.seek(logPosition);
        } catch (IOException e) {
            Exit.systemExit(e);
        }
        rewind();
    }
//    逐条解析并返回日志数据 在BadTail处返回null
    private byte[] internNext() {
        if (logPosition + LOG_DATA_OFFSET >= fileSize) {
            return null;
        }
         ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(logPosition);
            fc.read(buf);
        } catch(IOException e) {
            Exit.systemExit(e);
        }
        int size = Parser.byte2Int(buf.array());
        if (logPosition + size + LOG_DATA_OFFSET > fileSize) {
            return null;
        }
        ByteBuffer buf2 = ByteBuffer.allocate(LOG_DATA_OFFSET + size);
        try {
            fc.position(logPosition);
            fc.read(buf2);
        } catch(IOException e) {
            Exit.systemExit(e);
        }
        byte[] logData = buf2.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(logData, LOG_DATA_OFFSET, logData.length));
        int checkSum2 = Parser.byte2Int(Arrays.copyOfRange(logData, LOG_CHECK_SUM_OFFSET, LOG_DATA_OFFSET));
        if (checkSum1 != checkSum2) {
            return null;
        }
        logPosition += logData.length;
        return logData;
    }
//    校验和计算
    private int calChecksum(int checkValue, byte[] logData) {
        for (byte logDatum : logData) {
            checkValue = checkValue * LOG_CHECK_SUM_SEED + logDatum;
        }
        return checkValue;
    }
//将某条数据封装为日志格式插入日志文件,并更新总校验和
    @Override
    public void log(byte[] data) {
        byte[] logData = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(logData);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
            updateXCheckSum(logData);
        } catch(IOException e) {
            Exit.systemExit(e);
        } finally {
            lock.unlock();
        }
    }
//解析出日志数据部分
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, LOG_DATA_OFFSET, log.length);
        } finally {
            lock.unlock();
        }
    }
//    更新总校验和
    private void updateXCheckSum(byte[] logData) {
        this.xCheckSum=calChecksum(this.xCheckSum, logData);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
            fc.force(false);
        } catch (IOException e) {
            Exit.systemExit(e);
        }
    }
//  包装数据为日志格式
    private byte[] wrapLog(byte[] data) {
        int checksum = calChecksum(0, data);
        byte[] bytes = Parser.int2Byte(checksum);
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, bytes,data);
    }
//    截断Badtail
    @Override
    public void truncate(long x) {
        lock.lock();
        try {
            fc.truncate(x);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
    @Override
    public void rewind() {
        logPosition=4;
    }
    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Exit.systemExit(e);
        }
    }
}
