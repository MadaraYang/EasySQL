package com.madara.server.dm;

import com.google.common.primitives.Bytes;
import com.madara.common.SubArray;
import com.madara.exception.Exit;
import com.madara.server.dm.dataItem.DataItem;
import com.madara.server.dm.logger.Logger;
import com.madara.server.dm.page.Page;
import com.madara.server.dm.page.impl.XPage;
import com.madara.server.dm.pageCache.PageCache;
import com.madara.server.tm.TransactionManager;
import com.madara.utils.Parser;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

import static com.madara.common.Constants.*;

@Slf4j
public class Recover {
    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_INSERT_TYPE;
    }
    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache) {
        log.info("Recovering...");
        logger.rewind();
        int maxPgNo = 0;
        while (true) {
            byte[] logData = logger.next();
            if (logData == null) {
                break;
            }
            int pgNo;
            if (isInsertLog(logData)) {
                InsertLogInfo insertLogInfo = parseInsertLog(logData);
                pgNo = insertLogInfo.pgno;
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(logData);
                pgNo = updateLogInfo.pgno;
            }
            if (pgNo > maxPgNo) {
                maxPgNo = pgNo;
            }
        }
        if(maxPgNo == 0) {
            maxPgNo = 1;
        }
        pageCache.truncateByBgno(maxPgNo);
        log.info("Truncate Page Size:{}",maxPgNo);
        redoTranscations(tm, logger,pageCache);
        log.info("Redo Transactions Over.");
        undoTransactions(tm, logger, pageCache);
        log.info("Undo Transactions Over");
        }

    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        byte[] logTypeRaw = {LOG_INSERT_TYPE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(XPage.getFreeSpaceOffset(page));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }
    private static void undoTransactions(TransactionManager tm, Logger log, PageCache pageCache) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        log.rewind();
        while(true) {
            byte[] logData = log.next();
            if(logData == null) break;
            if(isInsertLog(logData)) {
                InsertLogInfo li = parseInsertLog(logData);
                long xid = li.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(logData);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(logData);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(logData);
                }
            }
        }

        // 对所有active log进行倒序undo
        for(Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] logData = logs.get(i);
                if(isInsertLog(logData)) {
                    doInsertLog(pageCache, logData, UNDO_FLAG);
                } else {
                    doUpdateLog(pageCache, logData, UNDO_FLAG);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static void redoTranscations(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();
        while(true) {
            byte[] log = logger.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pageCache, log, REDO_FLAG);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pageCache, log, REDO_FLAG);
                }
            }
        }

    }

    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO_FLAG) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pageCache.getPage(pgno);
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        try {
            XPage.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pageCache.getPage(li.pgno);
        } catch(Exception e) {
            Exit.systemExit(e);
        }
        try {
            if(flag == UNDO_FLAG) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            XPage.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.byte2Long(Arrays.copyOfRange(log, LOG_INSERT_XID_OFFSET, LOG_INSERT_PAGENO_OFFSET));
        insertLogInfo.pgno = Parser.byte2Int(Arrays.copyOfRange(log, LOG_INSERT_PAGENO_OFFSET, LOG_INSERT_OFFSET_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, LOG_INSERT_OFFSET_OFFSET, LOG_INSERT_DATA_OFFSET));
        insertLogInfo.raw = Arrays.copyOfRange(log, LOG_INSERT_DATA_OFFSET, log.length);
        return insertLogInfo;
    }
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.byte2Long(Arrays.copyOfRange(log, LOG_UPDATA_XID_OFFSET, LOG_UPDATA_UID_OFFSET));
        long uid = Parser.byte2Long(Arrays.copyOfRange(log, LOG_UPDATA_UID_OFFSET, LOG_UPDATA_RAW_OFFSET));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - LOG_UPDATA_RAW_OFFSET) / 2;
        li.oldRaw = Arrays.copyOfRange(log, LOG_UPDATA_RAW_OFFSET, LOG_UPDATA_RAW_OFFSET+length);
        li.newRaw = Arrays.copyOfRange(log, LOG_UPDATA_RAW_OFFSET+length, LOG_UPDATA_RAW_OFFSET+length*2);
        return li;
    }

    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logType = {LOG_UPDATE_TYPE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType,xidRaw,uidRaw,oldRaw,newRaw);
    }
}
