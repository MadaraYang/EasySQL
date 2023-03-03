package com.madara.server.dm.pageCache;

import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.server.dm.page.Page;
import com.madara.server.dm.pageCache.impl.PageCacheImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.madara.common.Constants.*;

public interface PageCache {

    int newPage(byte[] pageData);

    Page getPage(int pageNo);

    void close();

    void release(Page page);

    int getMaxPageNo();
    public void truncateByBgno(int maxPgno);
    void flushPage(Page page);
    public static PageCacheImpl create(String path,long memory) {
        File file = new File(path + DB_SUFFIX);
        try {
            if (!file.createNewFile()) {
                throw Error.FileExistsException;
            }
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        RandomAccessFile frw = null;
        FileChannel fc = null;
        try {
            frw = new RandomAccessFile(file, "rw");
            fc = frw.getChannel();
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        return new PageCacheImpl(frw,fc,(int)memory/PAGE_SIZE);
    }

    public static PageCacheImpl open(String path,long memory) {
        File f = new File(path+DB_SUFFIX);
        if(!f.exists()) {
            Exit.systemExit(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Exit.systemExit(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile frw = null;
        try {
            frw = new RandomAccessFile(f, "rw");
            fc = frw.getChannel();
        } catch (FileNotFoundException e) {
            Exit.systemExit(e);
        }
        return new PageCacheImpl(frw, fc,(int)memory/PAGE_SIZE);
    }
}
