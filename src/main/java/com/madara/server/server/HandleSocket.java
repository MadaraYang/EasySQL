package com.madara.server.server;

import com.madara.server.tbm.TableManager;
import com.madara.transport.Encoder;
import com.madara.transport.Package;
import com.madara.transport.Packager;
import com.madara.transport.Transporter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

@Slf4j
public class HandleSocket implements Runnable{
    private Socket socket;
    private TableManager tbm;
    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }
    @Override
    public void run() {
        InetSocketAddress address = (InetSocketAddress) socket.getRemoteSocketAddress();
        log.info("Establish connetction: {} : {}", address.getAddress().getHostAddress(), address.getPort());
        Transporter transporter = null;
        try {
            transporter = new Transporter(socket);
        } catch (IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            return;
        }
        Encoder encoder = new Encoder();
        Packager packager = new Packager(transporter, encoder);
        Executor exe = new Executor(tbm);
        while(true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch(Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                result = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
