package com.dbs.chord;

import com.dbs.chord.operations.OperationEntry;
import com.dbs.chord.operations.SuccessorRequestOperationEntry;
import com.dbs.network.Listener;
import com.dbs.network.messages.FindSuccessorMessage;
import com.dbs.network.messages.SuccessorMessage;
import com.dbs.utils.ConsoleLogger;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.security.NoSuchAlgorithmException;
import java.util.NavigableSet;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Node implements Chord{

    private static final int NUM_BITS_KEYS = 256;
    private static final int THREAD_POOL_SIZE = 10;
    private static final int REQUEST_TIMEOUT_MS = 3000;


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
     * Returns the successor's NodeInfo for given key
     * @param key - key to search
     * @return NodeInfo of successor's node
     */
    @Override
    public NodeInfo findSuccessor(BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        //iniciar pedido, com ref a addr deste node. quando for para responder, usar addr fornecido no pedido

        //if key > node && key <= successor
        if(key.compareTo(this.nodeInfo.id) > 0 && key.compareTo(this.successor.id) <= 0) {
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


        ServerSocket tempSocket = new ServerSocket(0);
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
    public void handleSuccessorRequest(SimpleNodeInfo originNode, BigInteger key) throws IOException, NoSuchAlgorithmException {
        ConsoleLogger.log(Level.INFO, "RECEIVED A HANDLE SUCCESSOR REQUEST!!!");


        //esta func lida com pedidos recebidos no listener, respondendo accordingly
        //precisa de ver qual o target para propagar se necessario node com base em fingertable


        //PERHAPS (REFACTOR -? maybe no need to actually) FIND_SUCCESSOR TO USE HERE?

        //if this is the starter node, it is responsible for any key for now
        if(this.successor.equals(this.nodeInfo)) {
            NodeInfo asker = new NodeInfo(originNode);

            SuccessorMessage msg = new SuccessorMessage(new SimpleNodeInfo(this.nodeInfo));
            this.nodeInfo.communicator.send(asker.getClientSocket(), msg);

        }
        //Missing conditions to be the successor
        //Else propagate to other target, based on fingerTable (call closestPrecedingNode)
    }

    @Override
    public void create() {
        this.successor = this.nodeInfo;
        this.predecessor = null;

        this.fingerTable.put(1, successor);
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

    private void startListening() throws IOException {
        ServerSocket s = new ServerSocket(nodeInfo.port);
        this.nodeInfo.setServerSocket(s);
        this.listener = new Listener(this);
        
        ExecutorService executorService = Executors.newFixedThreadPool(1);

        Future listenFuture = executorService.submit(() -> listener.listen(nodeInfo.communicator));
    }
}
