package com.dbs.network;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.network.messages.ChordMessage;
import com.dbs.network.messages.FindSuccessorMessage;
import com.dbs.network.messages.NodeInfoMessage;
import com.dbs.network.messages.SuccessorMessage;
import com.dbs.utils.ConsoleLogger;

import javax.net.ssl.SSLServerSocket;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;
import java.util.logging.Level;

public class Listener {

    private Node node;

    public Listener(Node node) {
        this.node = node;
    }

    public void listen(Communicator communicator) {
        ConsoleLogger.log(Level.INFO, "Listening for messages on port " + communicator.getPort() +  "...");

        while(true) {
            try {
                ConsoleLogger.log(Level.SEVERE, "Listening again... On socket port " + communicator.getPort());

                Object o = communicator.receive();
                ConsoleLogger.log(Level.WARNING, "Received an Object...");


                this.node.getThreadPool().execute(()-> {
                    try {
                        MessageHandler.handle(o, this.node);
                    } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });


            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }


    public Future<NodeInfo> listenOnSocket(ScheduledExecutorService threadPool, SSLServerSocket s) {

        return threadPool.submit(new Callable<NodeInfo>() {
            @Override
            public NodeInfo call() throws IOException, NoSuchAlgorithmException, ClassNotFoundException {
                NodeInfo nodeInfo = new NullNodeInfo();

                Communicator communicator = new Communicator(s);

                Object o = communicator.receive();

                ConsoleLogger.log(Level.INFO, "Received a Message on port " + s.getLocalPort());

                NodeInfoMessage msg = (NodeInfoMessage) ChordMessage.fromObject(o);
                if (!(msg.getNode() instanceof NullSimpleNodeInfo)) {
                    nodeInfo = new NodeInfo(msg.getNode());
                }

                s.close();
                return nodeInfo;
            }
        });
    }

}
