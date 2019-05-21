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


        while(true) {
            try {
                Object o = communicator.receive();

                this.node.getThreadPool().execute(() -> {
                    try {
                        MessageHandler.handle(o, this.node);

                    } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });

            } catch (SocketTimeoutException e) {
//                System.out.println("SOCKET TIMEOUT ON COMM " + communicator);
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


                NodeInfoMessage msg = (NodeInfoMessage) ChordMessage.fromObject(o);
                if (!(msg.getNode() instanceof NullSimpleNodeInfo)) {
                    nodeInfo = new NodeInfo(msg.getNode());
                }
                msg.handle(node);


                return nodeInfo;
                } catch (SocketTimeoutException e) {
                    System.out.println("SOCKET TIMEOUT" + communicator);
                    s.close();
                    return new NullNodeInfo();
                }
            }
        });
    }

}
