package com.dbs.chord;

import com.dbs.network.Communicator;
import com.dbs.utils.ByteToHash;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class NodeInfo {
    public final BigInteger id;
    public final InetAddress address;
    public final int port;

    public NodeInfo(InetAddress address, int port) throws NoSuchAlgorithmException {
        this.address = address;
        this.port = port;

        if(address == null) {
            this.id = BigInteger.valueOf(-1);
        } else {
            this.id = generateId(address, port).mod(BigInteger.valueOf(2).pow(Chord.NUM_BITS_KEYS));
        }
    }

    public NodeInfo(SimpleNodeInfo simpleInfo) throws IOException, NoSuchAlgorithmException {
        this(simpleInfo.address, simpleInfo.port);
    }

    private static BigInteger generateId(InetAddress address, int port) throws NoSuchAlgorithmException {
        byte[] rawId = Arrays.copyOf(address.getAddress(), address.getAddress().length + 4);

        rawId[4] = (byte) (port >> 24);
        rawId[5] = (byte) (port >> 16);
        rawId[6] = (byte) (port >> 8);
        rawId[7] = (byte) port;

        return ByteToHash.convert(rawId, "SHA-256");
    }

}
