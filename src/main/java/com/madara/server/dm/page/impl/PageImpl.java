package com.madara.server.dm.page.impl;

import com.madara.server.dm.page.Page;
import com.madara.server.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;

public class PageImpl implements Page {

//    页号
    private int pageNumber;
//    该页数据
    private byte[] data;
//    该页是否为脏页
    private boolean isDirty;
    //    pageCache
    private PageCache pageCache;
    private Lock lock;
    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
    }
//    设置该页为脏页
    @Override
    public void setDirty(boolean dirty) {
        isDirty = true;
    }
//    判断是否脏页
    @Override
    public boolean isDirty() {
        return isDirty;
    }
//  获取页号
    @Override
    public int getPageNumber() {
        return pageNumber;
    }
//获取该页数据
    @Override
    public byte[] getData() {
        return data;
    }
//释放该页
    @Override
    public void release() {
        pageCache.release(this);
    }
}
