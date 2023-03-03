package com.madara.server.server;

import com.madara.server.tbm.TableManager;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Madara
 */
@Slf4j
public class Server {
    private int port;
    TableManager tbm;
    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        log.info("Server listen to port:" + port);
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20,
                    1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while (true) {
                Socket accept = serverSocket.accept();
                HandleSocket handleSocket = new HandleSocket(accept, tbm);
                tpe.execute(handleSocket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                serverSocket.close();
            } catch (IOException e) {}
        }
    }
}
