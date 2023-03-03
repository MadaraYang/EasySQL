package com.madara.server.dm;

import com.madara.common.BaseCache;
import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.server.dm.dataItem.DataItem;
import com.madara.server.dm.dataItem.impl.DataItemImpl;
import com.madara.server.dm.logger.Logger;
import com.madara.server.dm.page.Page;
import com.madara.server.dm.page.impl.FirstPage;
import com.madara.server.dm.page.impl.XPage;
import com.madara.server.dm.pageCache.PageCache;
import com.madara.server.dm.pageIndex.PageIndex;
import com.madara.server.dm.pageIndex.PageInfo;
import com.madara.server.tm.TransactionManager;
import com.madara.utils.Parser;

import static com.madara.common.Constants.*;
public class DataManagerImpl extends BaseCache<DataItem> implements DataManager {
    TransactionManager tm;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page firstPage;
    public DataManagerImpl( PageCache pageCache, Logger logger,TransactionManager tm) {
        super(0);
        this.tm = tm;
        this.pageCache = pageCache;
        this.logger = logger;
        this.pageIndex = new PageIndex();
    }
//    根据uid获取dataItem
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl) get(uid);
        if (!dataItem.isValid()) {
            dataItem.release();
            return null;
        }
        return dataItem;
    }
//    打开时加载第一页
    boolean loadCheckPageOne() {
        try {
            firstPage = pageCache.getPage(1);
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        return FirstPage.checkValidCheck(firstPage);
    }
//    创建初始化第一页
    public void initFirstPage() {
        int i = pageCache.newPage(FirstPage.initRaw());
        assert i == 1;
        try {
            firstPage = pageCache.getPage(i);
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        pageCache.flushPage(firstPage);
    }
//    初始化pageIndex
    void fillPageIndex() {
        int maxPageNo = pageCache.getMaxPageNo();
        for (int i = 2; i <= maxPageNo; i++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
            } catch (Exception e) {
                Exit.systemExit(e);
            }
            pageIndex.add(page.getPageNumber(), XPage.getFreeSpace(page));
            page.release();
        }
    }
//    生成update日志
    public void logDataItem(long xid, DataItem dataItem) {
        byte[] bytes = Recover.updateLog(xid, dataItem);
        logger.log(bytes);
    }
//    从页面管理中获取页，记录插入日志，将数据写入缓存并将页还回去
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pageIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pageCache.newPage(XPage.initRaw());
                pageIndex.add(newPgno, MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }
        Page page = null;
        int freeSpace = 0;
        try {
            page = pageCache.getPage(pi.pgNo);
            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);
            short offset = XPage.insert(page, raw);
            page.release();
            return Parser.parseUID(pi.pgNo, offset);
        } finally {
            if (page != null) {
                pageIndex.add(pi.pgNo, XPage.getFreeSpace(page));
            } else {
                pageIndex.add(pi.pgNo,freeSpace);
            }
        }
    }
//  从PageCache中获取对应页，从对应页及对应偏移中获取DataItem
    @Override
    protected DataItem getByKeyOfCache(long uid) {
        short offset=(short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pageNo=(int)(uid & ((1L << 32) - 1));
        Page page = pageCache.getPage(pageNo);
        return DataItem.parseDataItem(page, offset, this);
    }
//  对应页引用减一
    @Override
    protected void releaseByKeyOfCache(DataItem key) {
        key.page().release();
    }
//    释放资源，安全关闭日志，页面缓存，首页安全关闭。
    @Override
    public void close() {
        super.close();
        logger.close();
        FirstPage.setValidCheckClose(firstPage);
        firstPage.release();
        pageCache.close();
    }
//    uid引用减一
    public void releaseDataItem(DataItemImpl dataItem) {
        release(dataItem.getUid());
    }
}
