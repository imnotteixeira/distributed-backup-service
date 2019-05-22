package com.dbs.chord;

import com.dbs.backup.BackupManager;
import com.dbs.chord.operations.OperationEntry;
import com.dbs.chord.operations.SuccessorRequestOperationEntry;
import com.dbs.network.Listener;
import com.dbs.network.messages.FindSuccessorMessage;
import com.dbs.network.messages.SuccessorMessage;
import com.dbs.utils.ConsoleLogger;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.NavigableSet;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.dbs.chord.Utils.*;

public class Node implements Chord{


    private static final int THREAD_POOL_SIZE = 10;
    private static final int REQUEST_TIMEOUT_MS = 3000;

    private BackupManager backupManager;

    private ScheduledExecutorService threadPool;
    private Listener listener;

    private ConcurrentHashMap<OperationEntry, Future<NodeInfo>> ongoingOperations;
    private ConcurrentSkipListMap<Integer, NodeInfo> fingerTable;

    private NodeInfo nodeInfo;
    private NodeInfo predecessor;
    private NodeInfo successor;

    public Node(NodeInfo nodeInfo) throws IOException {

        this.initNode(nodeInfo);

        this.create();

    }
    public Node(NodeInfo nodeInfo, NodeInfo existingNode) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        this.initNode(nodeInfo);


        ConsoleLogger.log(Level.INFO, "Generated ID: " + nodeInfo.id);

        this.join(existingNode);
    }

    public Node(InetAddress address, int port, NodeInfo successor) throws NoSuchAlgorithmException, IOException, ExecutionException, InterruptedException {
        this(new NodeInfo(address, port), successor);
    }
    public Node(InetAddress address, int port) throws NoSuchAlgorithmException, IOException {
        this(new NodeInfo(address, port));
    }

    private void initNode(NodeInfo nodeInfo) throws IOException {
        this.nodeInfo = nodeInfo;

        final String nodeAP = this.nodeInfo.getAccessPoint();

        this.backupManager = new BackupManager(nodeAP);

        this.threadPool = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);

        this.fingerTable = new ConcurrentSkipListMap<>();
        this.ongoingOperations = new ConcurrentHashMap<>();

        this.startListening();
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + nodeInfo.id +
                '}';
    }

    /**
     * Returns the successor's NodeInfo for given key, or the next hop for the request to be forwarded
     * @param key - key to search
     * @return NodeInfo of successor's node
     */
    @Override
    public NodeInfo findSuccessor(BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        //if this is the starter node, it is responsible for any key for now
        if(this.successor.equals(this.nodeInfo)) {
            return this.nodeInfo;
        }

        //if this node currently has no predecessor and the key equals this node's id, this is the responsible node
        if(this.predecessor == null && key.equals(this.nodeInfo.id)) {
            return this.nodeInfo;
        }

        //if there is a predecessor and the key is between predecessor and current node, current node is responsible
        if(this.predecessor != null && between(key, predecessor.id, this.nodeInfo.id)) {
            return this.nodeInfo;
        }

        //if key > node && key <= successor
        if(between(key, this.nodeInfo.id, this.successor.id) || key.equals(this.successor.id)) {
            return this.successor;
        } else {
            NodeInfo nextNode = closestPrecedingNode(key);
            return this.requestSuccessor(nextNode, key);
        }

    }

    private NodeInfo closestPrecedingNode(BigInteger key) {
        NavigableSet<Integer> fingers = fingerTable.descendingKeySet();

        for (Integer finger : fingers) {

            if (this.fingerTable.get(finger).id.compareTo(this.nodeInfo.id) > 0
                    && this.fingerTable.get(finger).id.compareTo(key) < 0) {
                return this.fingerTable.get(finger);
            }
        }

        return this.nodeInfo;
    }

    /**
     * Inserts a Future in the successorRequests HashMap and waits for the future to resolve with given timeout
     * Sends a TCP message to target node to find the successor of key
     * When this node receives a msg regarding this request, the promise is resovled with the nodeinfo attached and this function returns
     * @param targetNode - node to request
     * @param key - key to find
     * @return
     */
    private NodeInfo requestSuccessor(NodeInfo targetNode, BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        //esta func cria uma promise e espera q alguem responda
        //no listener e preciso fazer logica para tratar desta promise


        //ServerSocket tempSocket = new ServerSocket(0);
        SSLServerSocket tempSocket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        FindSuccessorMessage msg = new FindSuccessorMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()), key);
        this.nodeInfo.communicator.send(targetNode.getClientSocket(), msg);

        ConsoleLogger.log(Level.INFO,"Listening for messages on port " + tempSocket.getLocalPort() +  " for " + REQUEST_TIMEOUT_MS + "ms...");
        Future<NodeInfo> request = this.listener.listenOnSocket(this.threadPool, tempSocket);

        this.ongoingOperations.put(new SuccessorRequestOperationEntry(key), request);

        return request.get();
    }

    /**
     * If this node is the successor, answers back to originNode
     * Ohterwise, forwards a TCP message to target node to find the successor of key
     * @param originNode - node that requested the information originally
     * @param key - key to find
     * @return
     */
    public void handleSuccessorRequest(SimpleNodeInfo originNode, BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        //esta func lida com pedidos recebidos no listener, respondendo accordingly
        //precisa de ver qual o target para propagar se necessario node com base em fingertable

        NodeInfo successor = this.findSuccessor(key);
        NodeInfo asker = new NodeInfo(originNode);

        //If we already know the actual successor
        if(successor.equals(this.nodeInfo) || successor.equals(this.successor)) {
            SuccessorMessage msg = new SuccessorMessage(new SimpleNodeInfo(successor));
            this.nodeInfo.communicator.send(asker.getClientSocket(), msg);
        } else { //else propagate to other target, based on fingerTable
            FindSuccessorMessage msg = new FindSuccessorMessage(originNode, key);
            this.nodeInfo.communicator.send(successor.getClientSocket(), msg);
        }


    }

    @Override
    public void create() {
        this.predecessor = null;
        this.setSuccessor(this.nodeInfo);
    }

    @Override
    public void join(NodeInfo existingNode) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {


        this.predecessor = null;

        this.successor = this.requestSuccessor(existingNode, this.nodeInfo.id);
        ConsoleLogger.log(Level.INFO, "Found a Successor :: " + this.successor.id);


        //isto ta mal, o nodeinfo passado, e apenas um node existente na rede, nao necessariamente o successor, e preciso ver qual o actual successor
        //notify(successorInfo);
    }

    @Override
    public void notify(NodeInfo successor) throws IOException {

    }

    @Override
    public void handleSucessorNotification(SimpleNodeInfo predecessor) {

    }

    private void startListening() throws IOException {
        //ServerSocket s = new ServerSocket(nodeInfo.port);
        SSLServerSocket s = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(nodeInfo.port);
        this.nodeInfo.setServerSocket(s);
        this.listener = new Listener(this);
        
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        Future listenFuture = executorService.submit(() -> listener.listen(nodeInfo.communicator));
    }

    public void setSuccessor(NodeInfo successor) {
        this.successor = successor;
        this.fingerTable.put(1, successor);
    }
}
