package com.madara.server.dm;

import com.madara.server.dm.dataItem.DataItem;
import com.madara.server.dm.logger.Logger;
import com.madara.server.dm.page.impl.FirstPage;
import com.madara.server.dm.pageCache.PageCache;
import com.madara.server.tm.TransactionManager;


public interface DataManager {
    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pageCache = PageCache.create(path, mem);
        Logger logger = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pageCache,logger, tm);
        dm.initFirstPage();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pageCache = PageCache.open(path, mem);
        Logger logger = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pageCache, logger, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, logger, pageCache);
        }
        dm.fillPageIndex();
        FirstPage.setValidCheckPage(dm.firstPage);
        dm.pageCache.flushPage(dm.firstPage);
        return dm;
    }

}
