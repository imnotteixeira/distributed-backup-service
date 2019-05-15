package com.dbs.chord;

import com.dbs.network.Communicator;
import com.dbs.utils.ByteToHash;

import java.io.IOException;
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
    public ServerSocket serverSocket;
    public Socket clientSocket;
    public Communicator communicator;

    public NodeInfo(InetAddress address, int port) throws NoSuchAlgorithmException, IOException {
        this.address = address;
        this.port = port;

        this.id = generateId(address, port);
    }

    private static BigInteger generateId(InetAddress address, int port) throws NoSuchAlgorithmException {
        byte[] rawId = Arrays.copyOf(address.getAddress(), address.getAddress().length + 4);

        rawId[4] = (byte) (port >> 24);
        rawId[5] = (byte) (port >> 16);
        rawId[6] = (byte) (port >> 8);
        rawId[7] = (byte) port;

        return ByteToHash.convert(rawId, "SHA-256");
    }

    public void setServerSocket(ServerSocket s) {
        this.serverSocket = s;
        createOrUpdateCommunicator(s);
    }

    public Socket getClientSocket() throws IOException {
        if(this.clientSocket == null) {
            this.clientSocket = new Socket(this.address, this.port);
        }
        return this.clientSocket;
    }

    private void createOrUpdateCommunicator(ServerSocket s) {
        if(communicator == null) {
            this.communicator = new Communicator(s);
        } else {
            this.communicator.setServerSocket(s);
        }
    }

}
