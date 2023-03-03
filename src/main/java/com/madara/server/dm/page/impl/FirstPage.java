package com.madara.server.dm.page.impl;

import com.madara.server.dm.page.Page;
import com.madara.utils.RandomUtil;
import com.sun.org.apache.regexp.internal.RE;

import java.util.Arrays;

import static com.madara.common.Constants.PAGE_SIZE;
import static com.madara.common.Constants.VALID_CHECK_LEN;
import static com.madara.common.Constants.VALID_CHECK_OFFSET;


/**
 * @author Madara
 * 对首页进行管理
 * 判断是否正常关闭
 */
public class FirstPage {
    public static byte[] initRaw() {
        byte[] bytes = new byte[PAGE_SIZE];
        return bytes;
    }

    public static void setValidCheckPage(Page page) {
        page.setDirty(true);
        setValidCheckBytes(page.getData());
    }

    public static void setValidCheckBytes(byte[] bytes) {
        System.arraycopy(RandomUtil.randomBytes(VALID_CHECK_LEN), 0, bytes, VALID_CHECK_OFFSET, VALID_CHECK_LEN);
    }

    public static void setValidCheckClose(Page page) {
        setValidCheckClose(page.getData());
    }
    public static void setValidCheckClose(byte[] bytes) {
        System.arraycopy(bytes,VALID_CHECK_OFFSET , bytes, VALID_CHECK_OFFSET+VALID_CHECK_LEN, VALID_CHECK_LEN);
    }

    public static boolean checkValidCheck(Page page) {
        byte[] data = page.getData();
        return Arrays.equals(Arrays.copyOfRange(data, VALID_CHECK_OFFSET, VALID_CHECK_OFFSET+VALID_CHECK_LEN), Arrays.copyOfRange(data, VALID_CHECK_OFFSET+VALID_CHECK_LEN, VALID_CHECK_OFFSET+2*VALID_CHECK_LEN));
    }
}
