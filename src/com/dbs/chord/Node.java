package com.dbs.chord;

import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Arrays;

public class Node {
    private BigInteger id;

    public Node(BigInteger id) {
        this.id = id;
    }

    public Node(InetAddress address, int port) {
        this(generateId(address, port));
    }

    private static BigInteger generateId(InetAddress address, int port) {
        byte[] idGenerator = Arrays.copyOf(address.getAddress(), address.getAddress().length + 4);

        idGenerator[4] = (byte) (port >> 24);
        idGenerator[5] = (byte) (port >> 16);
        idGenerator[6] = (byte) (port >> 8);
        idGenerator[7] = (byte) port;

        return ByteToHex.convert(idGenerator);
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                '}';
    }
}
