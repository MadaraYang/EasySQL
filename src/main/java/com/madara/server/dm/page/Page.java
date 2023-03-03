package com.madara.server.dm.page;
public interface Page {
    public void setDirty(boolean dirty);
    public boolean isDirty();
    public int getPageNumber();
    public byte[] getData();
    public void release();
}
