package com.madara.utils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Peek {
    public static String debugALL(RandomAccessFile file, FileChannel fc) throws IOException {
        long length = file.length();
        ByteBuffer buf = ByteBuffer.wrap(new byte[(int)length]);
        fc.read(buf);
        buf.flip();
        byte[] array = buf.array();
        String s = new String(array);
        return s;
    }
}
