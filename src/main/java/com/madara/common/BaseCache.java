package com.madara.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BaseCache<T> {
    private HashMap<Long,T> cache;
    private HashMap<Long,Boolean> isGetting;
    private HashMap<Long,Integer> reference;
    private final long maxResource;
    private AtomicLong resourcesCount=new AtomicLong(0);
    private Lock lock;
//    根据key获取实际资源操作
    protected abstract T getByKeyOfCache(long key) throws Exception;
//    释放缓存,同时写回资源
    protected abstract void releaseByKeyOfCache(T key);

    public BaseCache(long maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        isGetting = new HashMap<>();
        reference = new HashMap<>();
        lock = new ReentrantLock();
    }
//    根据key从缓存中获取资源
    public T get(long key) {
        while (true) {
            lock.lock();
            if (isGetting.getOrDefault(key, false)) {
                lock.unlock();
//                说明有其他线程将把该资源缓存进来  自旋
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
            }
            if (cache.containsKey(key)) {
//                cache中有  返回  并引用数加一
                T t = cache.get(key);
                reference.put(key, reference.get(key) + 1);
                lock.unlock();
                return t;
            }
//            没有 isGetting()中标记  然后去获取

            resourcesCount.getAndIncrement();
            isGetting.put(key, true);
            lock.unlock();
            break;
        }
        T res = null;
        try {
            res=getByKeyOfCache(key);
        } catch (Exception e) {
            resourcesCount.decrementAndGet();
            isGetting.remove(key);
            e.printStackTrace();
        }
        lock.lock();
        try {
            isGetting.remove(key);
            cache.put(key, res);
            reference.put(key, 1);
        }finally {
            lock.unlock();
        }
        return res;
    }
//    从缓存中释放资源 若释放后引用数为0,将其写回
    public void release(long key) {
        lock.lock();
        try {
            int referenceCount = reference.get(key) - 1;
            if (referenceCount == 0) {
                T t = cache.get(key);
                releaseByKeyOfCache(t);
                cache.remove(key);
                reference.remove(key);
                resourcesCount.decrementAndGet();
            } else {
                reference.put(key,referenceCount);
            }
        } finally {
            lock.unlock();
        }
    }
//关闭缓存  写回所有资源
    public void close() {
        lock.lock();
        try {
            Set<Long> keySet = cache.keySet();
            for (Long key : keySet) {
                T t = cache.get(key);
                releaseByKeyOfCache(t);
                cache.remove(key);
                reference.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }
}
