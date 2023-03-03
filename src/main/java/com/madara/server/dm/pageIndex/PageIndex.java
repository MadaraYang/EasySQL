package com.madara.server.dm.pageIndex;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static com.madara.common.Constants.*;

public class PageIndex {
//  将每页按剩余空间分到40个区间里

    private Lock lock;
    private LinkedList<PageInfo>[] lists;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new LinkedList[PAGE_INTERVALS_NUMBERS+1];
        for (int i = 0; i < lists.length; i++) {
            lists[i] = new LinkedList<>();
        }
    }

    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int nums = spaceSize / PAGE_INTERVALS_SIZE;
            if (nums < PAGE_INTERVALS_NUMBERS) {
                nums++;
            }
            while (nums <= PAGE_INTERVALS_NUMBERS) {
                if(lists[nums].size() == 0) {
                    nums ++;
                    continue;
                }
                return lists[nums].removeFirst();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public void add(int pgNo, int freeSpace) {
        lock.lock();
        try {
            int num = freeSpace / PAGE_INTERVALS_SIZE;
            lists[num].add(new PageInfo(pgNo, freeSpace));
        }finally {
            lock.unlock();
        }
    }
}
