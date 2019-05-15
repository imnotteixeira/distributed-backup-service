package com.dbs.chord;

import com.dbs.network.Listener;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

public class Node implements Chord{
    private final NodeInfo nodeInfo;
    private final ScheduledExecutorService threadPool;

    private static final int THREAD_POOL_SIZE = 10;

    private NodeInfo predecessor;
    private NodeInfo successor;

    public Node(NodeInfo nodeInfo) throws IOException {

        this.nodeInfo = nodeInfo;

        System.out.println("My IP is " + nodeInfo.address);
        System.out.println("My Port is " + nodeInfo.port);
        System.out.println("I have no successor");

        this.threadPool = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);

        this.startListening();

    }
    public Node(NodeInfo nodeInfo, NodeInfo successor) throws IOException {
        this(nodeInfo);
        this.successor = successor;
        System.out.println("My successor's IP is " + successor.address);
        System.out.println("My successor's Port is " + successor.port);
        this.join(successor);
    }

    public Node(InetAddress address, int port, NodeInfo successor) throws NoSuchAlgorithmException, IOException {
        this(new NodeInfo(address, port), successor);
    }
    public Node(InetAddress address, int port) throws NoSuchAlgorithmException, IOException {
        this(new NodeInfo(address, port));
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + nodeInfo.id +
                '}';
    }

    @Override
    public NodeInfo findSuccessor(BigInteger key) {
        return null;
    }

    @Override
    public void join(NodeInfo successorInfo) throws IOException {
        notify(successorInfo);
    }

    @Override
    public void notify(NodeInfo successor) throws IOException {

        nodeInfo.communicator.send(successor.getClientSocket(), "Yo, im ur predecessor now");
    }

    private void startListening() throws IOException {
        ServerSocket s = new ServerSocket(nodeInfo.port);
        this.nodeInfo.setServerSocket(s);

        ExecutorService executorService = Executors.newFixedThreadPool(1);


        Future listenFuture = executorService.submit(() -> Listener.listen(nodeInfo.communicator));
    }
}
