package com.madara.server.dm.dataItem.impl;

import com.madara.common.SubArray;
import com.madara.server.dm.DataManagerImpl;
import com.madara.server.dm.dataItem.DataItem;
import com.madara.server.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static com.madara.common.Constants.*;
public class DataItemImpl implements DataItem {
//    data
    private SubArray raw;
//    用来数据项恢复
    private byte[] oldRaw;
//    读写锁
    private Lock rLock;
    private Lock wLock;
//    DM模块
    private DataManagerImpl dm;
//    位置
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page page,long uid,DataManagerImpl dm ) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.page = page;
        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
        rLock = reentrantReadWriteLock.readLock();
        wLock = reentrantReadWriteLock.writeLock();
    }
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+DATAITEM_DATA_OFFSET, raw.end);
    }
    public boolean isValid() {
        return raw.raw[raw.start+DATAITEM_VALID_OFFSET] == (byte)0;
    }

    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw,raw.start,oldRaw,0,oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw,0,raw.raw,raw.start,oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
