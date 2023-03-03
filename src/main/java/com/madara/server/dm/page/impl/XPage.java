package com.madara.server.dm.page.impl;

import com.madara.server.dm.page.Page;
import com.madara.utils.Parser;

import java.util.Arrays;

import static com.madara.common.Constants.*;
public class XPage {
    public static byte[] initRaw() {
        byte[] data = new byte[PAGE_SIZE];
        setFreeSpaceOffset(data, FREE_SPACE_OFFSET);
        return data;
    }

    public static void setFreeSpaceOffset(byte[] raw, short offset) {
        System.arraycopy(Parser.short2Byte(offset), 0, raw, FREE_START, FREE_SPACE_OFFSET);
    }
//    获取Page偏移
    public static short getFreeSpaceOffset(Page pg) {
        return getFreeSpaceOffset(pg.getData());
    }

    private static short getFreeSpaceOffset(byte[] data) {
        return Parser.parseShort(Arrays.copyOfRange(data, 0, 2));
    }

    // 将raw插入pg中， 返回插入位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFreeSpaceOffset(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFreeSpaceOffset(pg.getData(), (short)(offset + raw.length));
        return offset;
    }
    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PAGE_SIZE - (int)getFreeSpaceOffset(pg.getData());
    }
    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        short rawFSO = getFreeSpaceOffset(pg);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        if(rawFSO < offset + raw.length) {
            setFreeSpaceOffset(pg.getData(), (short)(offset+raw.length));
        }
    }
    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        byte[] data = pg.getData();
        System.arraycopy(raw, 0, data, offset, raw.length);
    }
}
