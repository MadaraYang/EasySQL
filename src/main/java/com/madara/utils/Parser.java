package com.madara.utils;

import com.google.common.primitives.Bytes;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Parser {
    public static byte[] long2Byte(long value) {
        byte[] bytes = new byte[8];
        return ByteBuffer.wrap(bytes).putLong(value).array();
    }

    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }
    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static long parseUID(int pageNo, short offset) {
        long l0 = pageNo;
        long l1 = offset;
        return l0 << 32 | l1;
    }
    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }
    public static ParseStringRes parseString(byte[] raw) {
        int length = byte2Int(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }
    public static byte[] int2Byte(int value) {
        byte[] bytes = new byte[4];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        buf.order(ByteOrder.BIG_ENDIAN).putInt(value);
        return buf.array();
    }

    public static int byte2Int(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return buf.getInt();
    }

    public static long byte2Long(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        return buf.getLong();
    }
}

