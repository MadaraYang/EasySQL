package com.madara.server.vm;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import static com.madara.common.Constants.DEFAULT_XID;

public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction transaction = new Transaction();
        transaction.xid = xid;
        transaction.level = level;
        if (level != 0) {
            transaction.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                transaction.snapshot.put(x, true);
            }
        }
        return transaction;
    }

    public boolean isInSnapshot(long xid) {
        if (xid == DEFAULT_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
