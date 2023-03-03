package com.madara.server;

import com.madara.exception.Error;
import com.madara.exception.Exit;
import com.madara.server.dm.DataManager;
import com.madara.server.server.Server;
import com.madara.server.tbm.TableManager;
import com.madara.server.tm.TransactionManager;
import com.madara.server.vm.VersionManager;
import com.madara.server.vm.impl.VersionManagerImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import static com.madara.common.Constants.*;
@Slf4j
public class Launcher {
    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options,args);

        if(cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if(cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        log.info("Usage: launcher (open|create) DBPath");
    }
    public static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

     public static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(DEFAULT_PORT, tbm).start();
    }

    public static long parseMem(String memStr) {
        if(memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if(memStr.length() < 2) {
            Exit.systemExit(Error.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length()-2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length()-2));
        switch(unit) {
            case "KB":
                return memNum*KB;
            case "MB":
                return memNum*MB;
            case "GB":
                return memNum*GB;
            default:
                Exit.systemExit(Error.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
