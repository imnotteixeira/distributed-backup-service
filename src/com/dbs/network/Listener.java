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
import java.net.SocketTimeoutException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.*;
import java.util.logging.Level;

public class Listener {

    private Node node;

    public Listener(Node node) {
        this.node = node;
    }

    public void listen(Communicator communicator) {
        ConsoleLogger.log(Level.INFO, "called listen() ::  Listening for messages on port " + communicator.getPort() +  "...");

        while(true) {
            try {
                ConsoleLogger.log(Level.SEVERE, "Inside listen while()... About to call comunicator.receive()");

                Object o = communicator.receive();


                ConsoleLogger.log(Level.SEVERE, "Creating a new thread to process received stuff...");
                this.node.getThreadPool().execute(() -> {
                    try {
                        ConsoleLogger.log(Level.SEVERE, "Will call handle()");
                        MessageHandler.handle(o, this.node);

                        ConsoleLogger.log(Level.SEVERE, "thread that handles has finished");
                    } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });

            } catch (SocketTimeoutException e) {
                System.out.println("SOCKET TIMEOUT ON COMM " + communicator);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }


    public Future<NodeInfo> listenOnSocket(ScheduledExecutorService threadPool, SSLServerSocket s) {

        return threadPool.submit(new Callable<NodeInfo>() {
            @Override
            public NodeInfo call() throws IOException, NoSuchAlgorithmException, ClassNotFoundException, ExecutionException, InterruptedException {

                Communicator communicator = new Communicator(s);
                try {


                NodeInfo nodeInfo = new NullNodeInfo();



                Object o = communicator.receive();

                ConsoleLogger.log(Level.INFO, "Received a Message on port " + s.getLocalPort());

                NodeInfoMessage msg = (NodeInfoMessage) ChordMessage.fromObject(o);
                if (!(msg.getNode() instanceof NullSimpleNodeInfo)) {
                    nodeInfo = new NodeInfo(msg.getNode());
                }
                msg.handle(node);

                s.close();
                return nodeInfo;
                } catch (SocketTimeoutException e) {
                    System.out.println("SOCKET TIMEOUT ON COMM" + communicator);
                    return new NullNodeInfo();
                }
            }
        });
    }

}
