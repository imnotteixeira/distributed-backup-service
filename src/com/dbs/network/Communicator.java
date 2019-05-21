package com.dbs.network;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.network.messages.ChordMessage;
import com.dbs.network.messages.FindSuccessorMessage;
import com.dbs.network.messages.NodeInfoMessage;
import com.dbs.utils.ConsoleLogger;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.*;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
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

    public Future<NodeInfo> listenOnSocket(SSLServerSocket tempSocket) {
        return node.getThreadPool().submit(() -> {
            SSLSocket s;
            NodeInfo nodeInfo = new NullNodeInfo();
            try {
                s = (SSLSocket) tempSocket.accept();

                try{
                    ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                    Object o = in.readObject();
                    NodeInfoMessage msg = (NodeInfoMessage) ChordMessage.fromObject(o);

                    if(!(msg.getNode() instanceof NullSimpleNodeInfo)){
                        nodeInfo = new NodeInfo(msg.getNode());
                    }

                    msg.handle(node);
                }catch(IOException | ClassNotFoundException e){
                    e.printStackTrace();
                }

                s.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            return nodeInfo;
        });
    }

    public void send(SSLSocket targetSocket, ChordMessage msg) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(targetSocket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(targetSocket.getInputStream());

            out.writeObject(msg);
            targetSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
