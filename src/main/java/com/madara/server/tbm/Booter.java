package com.madara.server.tbm;

import com.madara.exception.Error;
import com.madara.exception.Exit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import static com.madara.common.Constants.BOOTER_SUFFIX;
import static com.madara.common.Constants.BOOTER_TMP_SUFFIX;

//uid of first table
public class Booter {
    String path;
    File file;

    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Exit.systemExit(Error.FileExistsException);
            }
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Exit.systemExit(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Exit.systemExit(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Exit.systemExit(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Exit.systemExit(e);
        }
        return buf;
    }

    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Exit.systemExit(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Exit.systemExit(Error.FileCannotRWException);
        }
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch(IOException e) {
            Exit.systemExit(e);
        }
        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch(IOException e) {
            Exit.systemExit(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Exit.systemExit(Error.FileCannotRWException);
        }
    }
}
