package com.madara.server.vm;

import com.madara.server.dm.DataManager;
import com.madara.server.tm.TransactionManager;
import com.madara.server.vm.impl.VersionManagerImpl;

public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    boolean delete(long xid, long uid) throws Exception;
    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);
    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
