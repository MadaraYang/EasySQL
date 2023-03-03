package com.madara.server.vm;

import com.madara.common.UnReentrantLock;
import com.madara.exception.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
*  死锁检测
*/
public class LockTable {
//    事务持有的资源记录
    private Map<Long, List<Long>> transHadUIDMap;
//    资源被事务持有的记录
    private Map<Long, Long> uidHasHadByTrans;
//    哪些事务在等待此资源
    private Map<Long, List<Long>> uidWaitedTransMap;
//    事务阻塞在哪个锁
    private Map<Long, Lock> transWaitingLockMap;
//    事务正在等待的资源
    private Map<Long, Long> transWaitingUID;
    private Lock lock;

    public LockTable() {
        transHadUIDMap = new HashMap<>();
        uidHasHadByTrans = new HashMap<>();
        uidWaitedTransMap = new HashMap<>();
        transWaitingLockMap = new HashMap<>();
        transWaitingUID = new HashMap<>();
        lock = new ReentrantLock();
    }
//    不需要等待返回null，否则返回锁，如果死锁抛异常
    public Lock add(long xid,long uid) throws  Exception {
        lock.lock();
        try {
            if (isInList(transHadUIDMap, xid, uid)) {
                return null;
            }
            if (!uidHasHadByTrans.containsKey(uid)) {
                uidHasHadByTrans.put(uid, xid);
                putIntoList(transHadUIDMap,xid,uid);
                return null;
            }
            transWaitingUID.put(xid, uid);
            putIntoList(uidWaitedTransMap,uid,xid);
            if (hasDeadLock()) {
                transWaitingUID.remove(xid);
                removeFromList(uidWaitedTransMap,uid,xid);
                throw Error.DeadlockException;
            }
            UnReentrantLock l = new UnReentrantLock();
            l.lock();
            transWaitingLockMap.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    private void selectNewXID(long uid) {
        uidHasHadByTrans.remove(uid);
        List<Long> xids = uidWaitedTransMap.get(uid);
        if (xids == null) {
            return;
        }
        assert xids.size() > 0;
        while(xids.size() > 0) {
            long xid = xids.remove(0);
            if(!transWaitingLockMap.containsKey(xid)) {
                continue;
            } else {
                uidHasHadByTrans.put(uid, xid);
                Lock lo = transWaitingLockMap.remove(xid);
                transWaitingUID.remove(xid);
                lo.unlock();
                break;
            }
        }
        if (xids.size() == 0) {
            uidWaitedTransMap.remove(uid);
        }
    }

    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = transHadUIDMap.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            transWaitingUID.remove(xid);
            transHadUIDMap.remove(xid);
            transWaitingLockMap.remove(xid);
        }finally {
            lock.unlock();
        }
    }
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) {
            return;
        }
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }
    private Map<Long, Integer> xidStamp;
    private int stamp;
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : transHadUIDMap.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);
        Long uid = transWaitingUID.get(xid);
        if (uid == null) {
            return false;
        }
        Long x = uidHasHadByTrans.get(uid);
        assert x != null;
        return dfs(x);
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long xid, long uid) {
        if (!listMap.containsKey(xid)) {
            listMap.put(xid, new ArrayList<>());
        }
        listMap.get(xid).add(uid);
    }
    private boolean isInList(Map<Long, List<Long>> listMap, long xid, long uid1) {
        List<Long> l = listMap.get(xid);
        if (l == null) {
            return false;
        }
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
