package com.madara.common;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class UnReentrantLock implements Lock {
//    @Test
//    public void t() {
//        UnReentrantLock lock = new UnReentrantLock();
//        lock.lock();
//        lock.lock();
//        lock.unlock();
//        lock.unlock();
//        System.out.println(1);
//    }
    private Sync innerSync = new Sync();
    static class Sync extends AbstractQueuedSynchronizer{
        @Override
        protected boolean tryAcquire(int arg) {
            if (compareAndSetState(0, 1)) {
                setExclusiveOwnerThread(Thread.currentThread());
                return true;
            }
            return false;
        }
        @Override
        protected boolean tryRelease(int arg) {
            int c = getState() - 1;
            boolean res = false;
            if (c == 0) {
                setExclusiveOwnerThread(null);
                setState(0);
                res = true;
            }
            return false;
        }
        @Override
        protected boolean isHeldExclusively() {
            return getState() == 1;
        }
    }
    @Override
    public void lock() {
        innerSync.acquire(1);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        innerSync.acquire(1);
    }

    @Override
    public boolean tryLock() {
        return innerSync.tryAcquire(1);
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return innerSync.tryAcquireNanos(1,unit.toNanos(time));
    }

    @Override
    public void unlock() {
        innerSync.release(1);
    }

    @Override
    public Condition newCondition() {
        return null;
    }
}
