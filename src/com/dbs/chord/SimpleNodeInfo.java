package com.dbs.chord;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;

public class SimpleNodeInfo implements Serializable {
    public final InetAddress address;
    public final int port;

    public SimpleNodeInfo(InetAddress address, int port) throws NoSuchAlgorithmException, IOException {
        this.address = address;
        this.port = port;
    }

    public SimpleNodeInfo(NodeInfo nodeInfo) throws IOException, NoSuchAlgorithmException {
        this(nodeInfo.address, nodeInfo.port);
    }
}
