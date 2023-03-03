package com.madara.server.vm.impl;

import com.madara.common.BaseCache;
import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.server.dm.DataManager;
import com.madara.server.tm.TransactionManager;
import com.madara.server.vm.*;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.madara.common.Constants.*;
/**
 * @author Madara
 */
@Slf4j
public class VersionManagerImpl extends BaseCache<Entry> implements VersionManager {
    public TransactionManager tm;
    public DataManager dm;
    public Map<Long, Transaction> activeTransaction;
    private Lock lock;
    private LockTable lockTable;
    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        activeTransaction = new HashMap<>();
        activeTransaction.put(DEFAULT_XID, Transaction.newTransaction(DEFAULT_XID, 0, null));
        lock = new ReentrantLock();
        lockTable = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                l = lockTable.add(xid, uid);
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction transaction = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, transaction);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();
        try {
            if (transaction.err != null) {
                throw transaction.err;
            }
        } catch (NullPointerException n) {
            log.info("{}",xid);
            log.info("{}",activeTransaction.keySet().toString());
            Exit.systemExit(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if(t.autoAborted) {
            return;
        }
        lockTable.remove(xid);
        tm.abort(xid);
    }
    @Override
    protected Entry getByKeyOfCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry==null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseByKeyOfCache(Entry key) {
        key.remove();
    }
    public void releaseEntry(Entry entry){
        super.release(entry.getUid());
    }

}
