package com.madara.server.vm;

import com.google.common.primitives.Bytes;
import com.madara.common.SubArray;
import com.madara.server.dm.dataItem.DataItem;
import com.madara.server.vm.impl.VersionManagerImpl;
import com.madara.utils.Parser;

import java.util.Arrays;

import static com.madara.common.Constants.*;
public class Entry {
    private long uid;
    private DataItem dataItem;
    private VersionManagerImpl vm;

    public static Entry newEntry(VersionManagerImpl vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }
    public static Entry loadEntry(VersionManagerImpl vm, long uid) throws Exception {
        DataItem dataItem = vm.dm.read(uid);
        return newEntry(vm, dataItem, uid);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        vm.releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray origin = dataItem.data();
            byte[] data = new byte[origin.end - origin.start - ENTRY_DATA_OFFSET];
            System.arraycopy(origin.raw,origin.start+ENTRY_DATA_OFFSET,data,0,data.length);
            return data;
        }finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray origin = dataItem.data();
            return Parser.byte2Long(Arrays.copyOfRange(origin.raw, origin.start + ENTRY_CREATER_XID_OFFSET, origin.start + ENTRY_DELETER_XID_OFFSET));
        }finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray origin = dataItem.data();
            return Parser.byte2Long(Arrays.copyOfRange(origin.raw, origin.start + ENTRY_DELETER_XID_OFFSET, origin.start + ENTRY_DATA_OFFSET));
        }finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray origin = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid),0,origin.raw,origin.start+ENTRY_DELETER_XID_OFFSET,8);
        }finally {
            dataItem.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }
}
