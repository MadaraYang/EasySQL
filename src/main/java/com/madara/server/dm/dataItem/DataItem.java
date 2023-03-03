package com.madara.server.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.madara.common.SubArray;
import com.madara.server.dm.DataManagerImpl;
import com.madara.server.dm.dataItem.impl.DataItemImpl;
import com.madara.server.dm.page.Page;
import com.madara.utils.Parser;

import static com.madara.common.Constants.*;
import java.util.Arrays;

public interface DataItem {


    SubArray data();
    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

//    将raw数据包装为文件中的数据项
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }
//    解析页中offset处数据项
    public static DataItem parseDataItem(Page pageNo, short offset, DataManagerImpl dm) {
        byte[] raw = pageNo.getData();
        short dataSize = Parser.parseShort(Arrays.copyOfRange(raw, offset + DATAITEM_SIZE_OFFSET, offset + DATAITEM_DATA_OFFSET));
        short rawSize = (short) (dataSize + DATAITEM_DATA_OFFSET);
        long uid = Parser.parseUID(pageNo.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+rawSize),new byte[rawSize],pageNo,uid,dm);
    }
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DATAITEM_VALID_OFFSET] = (byte) 1;
    }
}
