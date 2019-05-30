package com.dbs.network;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.network.messages.ChordMessage;
import com.dbs.network.messages.FindSuccessorMessage;
import com.dbs.network.messages.NodeInfoMessage;
import com.dbs.utils.ConsoleLogger;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;

public class Communicator {


    private SSLServerSocket serverSocket = null;
    private Node node;



    public Communicator(Node n, SSLServerSocket s) {
        this.node = n;
        this.serverSocket = s;
    }

    public void listen() {
        node.getThreadPool().submit(() -> {
            while(true){
                SSLSocket s = (SSLSocket) serverSocket.accept();

                node.getThreadPool().submit(() -> {
                    try {
                        ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                        ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                        Object o = in.readObject();
                        MessageHandler.handle(o, node);

                    } catch (SocketException e) {
                        ConsoleLogger.log(Level.WARNING, "Socket was closed unexpectedly");
                    } catch (IOException | ClassNotFoundException | InterruptedException | ExecutionException | NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }

                    try {
                        s.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        });
    }

    public Future<NodeInfo> listenOnSocket(SSLServerSocket tempSocket){
        return node.getThreadPool().submit(() -> {
            SSLSocket s;
            NodeInfo nodeInfo = new NullNodeInfo();
            s = (SSLSocket) tempSocket.accept();

            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(s.getInputStream());
            Object o = in.readObject();
            NodeInfoMessage msg = (NodeInfoMessage) ChordMessage.fromObject(o);

            if(!(msg.getNode() instanceof NullSimpleNodeInfo)){
                nodeInfo = new NodeInfo(msg.getNode());
            }

            msg.handle(node);

            s.close();

            return nodeInfo;
        });
    }

    public CompletableFuture<ChordMessage> async_listenOnSocket(SSLServerSocket tempSocket) {
        return CompletableFuture.supplyAsync(()->{
            SSLSocket s;
            try {
                s = (SSLSocket) tempSocket.accept();

                ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                Object o = in.readObject();
                return ChordMessage.fromObject(o);

            } catch (ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void send(SSLSocket targetSocket, ChordMessage msg) throws IOException{
        ObjectOutputStream out = new ObjectOutputStream(targetSocket.getOutputStream());
        ObjectInputStream in = new ObjectInputStream(targetSocket.getInputStream());

        out.writeObject(msg);
        targetSocket.close();

    }
}
