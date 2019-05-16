package com.dbs.network;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.network.messages.ChordMessage;
import com.dbs.network.messages.FindSuccessorMessage;
import com.dbs.network.messages.SuccessorMessage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

public class Listener {

    private Node node;

    public Listener(Node node) {
        this.node = node;
    }

    public void listen(Communicator communicator) {
        while(true) {
            try {
                System.out.println("Listening for messages on port " + communicator.getPort() +  "...");
                Object o = communicator.receive();

                System.out.println("Received an Object:");

                MessageHandler.handle(o, this.node);
            } catch (IOException | ClassNotFoundException | NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }


    public Future<NodeInfo> listenOnSocket(ScheduledExecutorService threadPool, ServerSocket s) {

        return threadPool.submit(new Callable<NodeInfo>() {
            @Override
            public NodeInfo call() throws IOException, NoSuchAlgorithmException {
                NodeInfo nodeInfo = new NullNodeInfo();
                try {
                    Communicator communicator = new Communicator(s);

                    Object o = communicator.receive();

                    System.out.println("Received an Object:");

                    SuccessorMessage msg = (SuccessorMessage) ChordMessage.fromObject(o);
                    nodeInfo = new NodeInfo(msg.getSuccessor());
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }

                return nodeInfo;
            }
        });
    }

}
