package com.madara.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.madara.client.Client;
import com.madara.client.Shell;
import com.madara.transport.Encoder;
import com.madara.transport.Packager;
import com.madara.transport.Transporter;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.*;

import static com.madara.common.Constants.DEFAULT_PORT;
import static com.madara.common.Constants.DEFAULT_SERVER_ADDRESS;
import static com.madara.server.Launcher.parseMem;

public class ExecutorTest {
    Socket socket;
    @Test
    public void t() throws InterruptedException, IOException, ExecutionException {
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
//        Future<?> submit = tpe.submit(() -> {
//            Launcher.createDB("E:/EasySQLTest/temp_test2");
//        });
//        submit.get();
        tpe.submit(() -> {
            Launcher.openDB("E:/EasySQLTest/temp_test2", parseMem("64MB"));
        });
        socket= new Socket(DEFAULT_SERVER_ADDRESS, DEFAULT_PORT);
        Thread.sleep(1000);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }

    @Test
    public void t2() throws InterruptedException, IOException {
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        tpe.submit(() -> {
            Launcher.openDB("E:/EasySQLTest/temp_test2", parseMem("64MB"));
        });
        Thread.sleep(1000);
        Encoder encoder = new Encoder();
        socket= new Socket(DEFAULT_SERVER_ADDRESS, DEFAULT_PORT);
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
