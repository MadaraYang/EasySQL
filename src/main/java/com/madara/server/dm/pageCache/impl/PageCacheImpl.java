package com.madara.server.dm.pageCache.impl;

import com.madara.common.BaseCache;
import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.server.dm.page.Page;
import com.madara.server.dm.page.impl.PageImpl;
import com.madara.server.dm.pageCache.PageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.madara.common.Constants.MEM_MIN_PAGE_SIZE;
import static com.madara.common.Constants.PAGE_SIZE;

/**
 * @author Madara
 */
public class PageCacheImpl extends BaseCache<Page> implements PageCache {
    private final RandomAccessFile file;
    private final FileChannel fc;
    private final int maxResource;
//    缓存中的总页数
    private AtomicInteger pageNumbers;
    private final Lock lock;
    public PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_PAGE_SIZE) {
            Exit.systemExit(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Exit.systemExit(e);
        }
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
        this.file = file;
        this.fc = fc;
        this.maxResource = maxResource;
        this.lock = new ReentrantLock();
    }

    private void flush(Page page) {
        int pageNo = page.getPageNumber();
        long pageOffset = getPageOffset(pageNo);
        lock.lock();
        try {
            byte[] data = page.getData();
            fc.position(pageOffset);
            ByteBuffer buf = ByteBuffer.wrap(data);
            fc.write(buf);
            fc.force(false);
        } catch (Exception e) {
            Exit.systemExit(e);
        } finally {
            lock.unlock();
        }
    }
    public void truncateByBgno(int maxPgno) {
        long size = getPageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Exit.systemExit(e);
        }
        pageNumbers.set(maxPgno);
    }
    private long getPageOffset(int pageNo) {
        return (pageNo - 1) * PAGE_SIZE;
    }
    @Override
    public int newPage(byte[] pageData) {
        int i = pageNumbers.incrementAndGet();
        PageImpl page = new PageImpl(i, pageData, null);
        flushPage(page);
        return i;
    }

    @Override
    public Page getPage(int pageNo) {
        return get(pageNo);
    }

//    根据页号获取Page
    @Override
    protected Page getByKeyOfCache(long key) {
        long pageOffset = getPageOffset((int)key);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        lock.lock();
        try {
            fc.position(pageOffset);
            fc.read(buf);
            byte[] array = buf.array();
            return new PageImpl((int) key, array, this);
        } catch (Exception e) {
            Exit.systemExit(e);
        } finally {
            lock.unlock();
        }
        return null;
    }

    @Override
    protected void releaseByKeyOfCache(Page page) {
        if (page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Exit.systemExit(e);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public int getMaxPageNo() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }
}
