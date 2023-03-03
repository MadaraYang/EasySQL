package com.madara.transport;

public class Packager {
    private Transporter transpoter;
    private Encoder encoder;
    public Packager(Transporter transpoter, Encoder encoder) {
        this.transpoter = transpoter;
        this.encoder = encoder;
    }
    public void close() throws Exception {
        transpoter.close();
    }
    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transpoter.send(data);
    }
    public Package receive() throws Exception {
//        从socket channel 中读取
        byte[] data = transpoter.receive();
        return encoder.decode(data);
    }
}
