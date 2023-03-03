package com.madara.client;

import com.madara.transport.Encoder;
import com.madara.transport.Packager;
import com.madara.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

import static com.madara.common.Constants.DEFAULT_PORT;
import static com.madara.common.Constants.DEFAULT_SERVER_ADDRESS;

/**
 * @author Madara
 */
public class Launcher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket(DEFAULT_SERVER_ADDRESS, DEFAULT_PORT);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);
        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
