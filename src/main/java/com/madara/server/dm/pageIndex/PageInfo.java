package com.madara.server.dm.pageIndex;

public class PageInfo {
    public int pgNo;
    public int freeSpace;

    public PageInfo(int pgNo, int freeSpace) {
        this.pgNo = pgNo;
        this.freeSpace = freeSpace;
    }
}
