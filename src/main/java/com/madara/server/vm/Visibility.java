package com.madara.server.vm;

import com.madara.server.tm.TransactionManager;

import java.util.concurrent.ThreadPoolExecutor;

//各隔离级别事务可见性判断
public class Visibility {
    public static boolean isVersionSkip(TransactionManager tm, Transaction transaction, Entry entry) {
        long xmax = entry.getXmax();
        if (transaction.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > transaction.xid || transaction.isInSnapshot(xmax));
        }
    }
    private static boolean readCommitted(TransactionManager tm, Transaction transaction, Entry entry) {
        long xmin = entry.getXmin();
        long xmax = entry.getXmax();
        if (xmin == transaction.xid&&xmax==0) {
            return true;
        }
        if (tm.isCommitted(xmin)) {
            if (xmax == 0) {
                return true;
            }
            if (xmax != transaction.xid && !tm.isCommitted(xmax)) {
                return true;
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction transaction, Entry entry) {
        long xid = transaction.xid;
        long xmin = entry.getXmin();
        long xmax = entry.getXmax();
        if (xid == xmin && xmax == 0) {
            return true;
        }
        if (tm.isCommitted(xmin) && xmin < xid && !transaction.isInSnapshot(xmin)) {
            if (xmax == 0) {
                return true;
            }
            if (xmax != xid) {
                if (!tm.isCommitted(xmax) || xmax > xid || transaction.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }
}
