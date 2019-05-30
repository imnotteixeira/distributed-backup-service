package com.dbs.chord;

import com.dbs.backup.BackupManager;
import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.operations.OperationEntry;
import com.dbs.chord.operations.PredecessorRequestOperationEntry;
import com.dbs.chord.operations.SuccessorRequestOperationEntry;
import com.dbs.network.Communicator;
import com.dbs.network.NullNodeInfo;
import com.dbs.network.messages.*;
import com.dbs.utils.ConsoleLogger;
import com.dbs.utils.State;

import javax.net.ssl.*;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.dbs.chord.Utils.*;

public class Node implements Chord{

    public static final int MAX_FILE_SIZE_BYTES = (int) (64 *10e6);
    public static final int REPLICATION_DEGREE = 3;

    private static final int THREAD_POOL_SIZE = 150;
    private static final int REQUEST_TIMEOUT_MS = 5000;
    private static final int STABILIZATION_INTERVAL_MS = 200;
    private static final int FIX_FINGER_INTERVAL_MS = 200;
    private static final int CHECK_PREDECESSOR_INTERVAL_MS = 200;
    private static String NODE_PATH;

    private BackupManager backupManager;


    private ScheduledExecutorService threadPool;
    private Communicator communicator;

    private ConcurrentHashMap<OperationEntry, Future<NodeInfo>> ongoingOperations;

    private ConcurrentSkipListMap<Integer, NodeInfo> fingerTable;

    private NodeInfo nodeInfo;
    private NodeInfo predecessor;
    private NodeInfo successor;

    private int nextFinger = 0;
    private State state;


    public Node(NodeInfo nodeInfo) throws IOException {

        this.initNode(nodeInfo);

        this.create();

    }

    public Node(NodeInfo nodeInfo, NodeInfo existingNode) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        this.initNode(nodeInfo);

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

        ConsoleLogger.log(Level.SEVERE, "My ID: " + nodeInfo.id);



        final String nodeAP = this.nodeInfo.getAccessPoint();

        Node.NODE_PATH = nodeAP;

        this.backupManager = new BackupManager(this);

        this.threadPool = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);

        this.fingerTable = new ConcurrentSkipListMap<>();
        this.ongoingOperations = new ConcurrentHashMap<>();

        this.state = new State();

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

        ConsoleLogger.log(Level.INFO, "Someone is looking for node responsible for " + key);

        //if this is the starter node, it is responsible for any key for now
        if(this.successor.id.equals(this.nodeInfo.id)) {
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

            if(between(this.fingerTable.get(finger).id, this.nodeInfo.id, key)) {
                return this.fingerTable.get(finger);
            }
        }

        return this.nodeInfo;
    }

    /**
     * Inserts a Future in the ongoingOperations Map and waits for the future to resolve with given timeout
     * Sends a TCP message to target node to find the successor of key
     * When this node receives a msg regarding this request, the promise is resovled with the nodeinfo attached and this function returns
     * @param targetNode - node to request
     * @param key - key to find
     * @return
     */
    private NodeInfo requestSuccessor(NodeInfo targetNode, BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        //ServerSocket tempSocket = new ServerSocket(0);
        SSLServerSocket tempSocket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        FindSuccessorMessage msg = new FindSuccessorMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()), key);

        SSLSocket targetSocket = (SSLSocket) SSLSocketFactory.getDefault().createSocket(targetNode.address, targetNode.port);


        ConsoleLogger.log(Level.INFO,"Listening for messages on port " + tempSocket.getLocalPort() +  " for " + REQUEST_TIMEOUT_MS + "ms...");

        Future<NodeInfo> request = this.communicator.listenOnSocket(tempSocket);
        this.communicator.send(targetSocket, msg);


        this.ongoingOperations.put(new SuccessorRequestOperationEntry(key), request);

        return request.get();
    }

    /**
     * If this node is the successor or the predecessor of successor, answers back to originNode
     * Ohterwise, forwards a TCP message to target node to find the successor of key
     * @param originNode - node that requested the information originally
     * @param key - key to find
     * @return
     */
    public void handleSuccessorRequest(SimpleNodeInfo originNode, BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {


        NodeInfo successor = this.findSuccessor(key);
        NodeInfo asker = new NodeInfo(originNode);

        //If we already know the actual successor
        if(successor.equals(this.nodeInfo) || successor.equals(this.successor)) {
            SuccessorMessage msg = new SuccessorMessage(key, new SimpleNodeInfo(successor));
            this.communicator.send(Utils.createClientSocket(asker.address, asker.port), msg);
        } else { //else propagate to other target, based on fingerTable
            FindSuccessorMessage msg = new FindSuccessorMessage(originNode, key);
            this.communicator.send(Utils.createClientSocket(successor.address, successor.port), msg);
        }

    }

    @Override
    public void create() {
        this.setPredecessor(null);
        this.setSuccessor(this.nodeInfo);

        this.bootstrapStabilizer();
        this.bootstrapFixFingers();
        this.bootstrapCheckPredecessor();
    }

    @Override
    public void join(NodeInfo existingNode) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        this.setPredecessor(null);

        NodeInfo succ = this.requestSuccessor(existingNode, this.nodeInfo.id);
        ConsoleLogger.log(Level.INFO, "I was assigned the successor: " + succ.id);

        this.setSuccessor(succ);

        this.bootstrapStabilizer();
        this.bootstrapFixFingers();
        this.bootstrapCheckPredecessor();

    }

    @Override
    public void notify(NodeInfo successor) throws IOException, NoSuchAlgorithmException {

        ConsoleLogger.log(Level.INFO, "Notifying successor " + successor.id + " on port " + successor.port);

        NotifySuccessorMessage msg = new NotifySuccessorMessage(new SimpleNodeInfo(this.nodeInfo));

        this.communicator.send(Utils.createClientSocket(successor.address, successor.port), msg);

    }

    /**
     * potentialPredecessor thinks it might be this node's predecessor
     * @param potentialPredecessorInfo
     */
    @Override
    public void handlePredecessorNotification(SimpleNodeInfo potentialPredecessorInfo) throws IOException, NoSuchAlgorithmException {
        ConsoleLogger.log(Level.WARNING, "Received a potential predecessor NOTIFICATION from node at port " + potentialPredecessorInfo.port);
        NodeInfo potentialPredecessor = new NodeInfo(potentialPredecessorInfo);

        if(this.predecessor == null || this.predecessor.id.equals(this.nodeInfo.id) || between(potentialPredecessor.id, this.predecessor.id, this.nodeInfo.id)) {
            this.setPredecessor(potentialPredecessor);
        }

    }

    /**
     * Called periodically. verifies this node's immediate successor, and tells the successor about itself.
     */
    @Override
    public void stabilize() throws IOException, InterruptedException, NoSuchAlgorithmException {
        ConsoleLogger.log(Level.INFO, "Stabilizing Network...");

        NodeInfo x;
        if (this.successor.id.equals(this.nodeInfo.id)) {

            if(this.predecessor == null) return;

            x = this.predecessor;

        } else {
            ConsoleLogger.log(Level.INFO, "Will request predecessor of " + this.successor.id);
            try {
                x = requestPredecessor(this.successor);
            }catch(ExecutionException | SocketException e){
                //if this request fails, it means my successor prolly is offline, must update stuffs
                this.handleSuccessorFail();
                return;
            }
        }


        if(!(x instanceof NullNodeInfo)) {
            if(!x.id.equals(this.successor.id) && between(x.id, this.nodeInfo.id, this.successor.id)) {
                ConsoleLogger.log(Level.SEVERE, "Here");
                this.setSuccessor(x);
            } else if(this.successor.id.equals(this.nodeInfo.id) && this.predecessor != null) { // when I have a predecessor (newly joined node) but it should be my successor
                ConsoleLogger.log(Level.SEVERE, "There");
                this.setSuccessor(this.predecessor);
            }
        }



        try {
            ConsoleLogger.log(Level.SEVERE, "Notifying successor "+ this.successor);
            this.notify(this.successor);
        } catch (Exception e){
            //Successor was removed before sending notification
        }

    }

    @Override
    public void handleSuccessorFail(){
        ConsoleLogger.log(Level.SEVERE, "Successor offline - Fault tolerance not implemented");
    }


    @Override
    public synchronized void checkPredecessor() throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        if(this.predecessor == null || this.predecessor.id.equals(this.nodeInfo.id)) return;

        SSLServerSocket tempSocket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        StatusCheckMessage msg = new StatusCheckMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()));

        try {

            Future<NodeInfo> request = this.communicator.listenOnSocket(tempSocket);

            this.communicator.send(Utils.createClientSocket(this.predecessor.address, this.predecessor.port), msg);

            ConsoleLogger.log(Level.INFO, "Sent predecessor status check for node: " + this.predecessor.id);

            request.get();

        } catch(Exception e) {
            ConsoleLogger.log(Level.SEVERE, "Predecessor went offline!");
            if(this.predecessor.id.equals(this.successor.id)){
                this.setSuccessor(new NullNodeInfo());
            }
            this.setPredecessor(null);
        }
    }

    @Override
    public void fixFingers() throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException {
        this.nextFinger += 1;
        if(this.nextFinger > Chord.NUM_BITS_KEYS){
            this.nextFinger = 1;
        }
        fingerTable.put(this.nextFinger,
                this.findSuccessor(
                        this.nodeInfo.id
                                .add(BigInteger.valueOf(2)
                                        .pow(this.nextFinger - 1))
                                .mod(BigInteger.valueOf(2)
                                        .pow(Chord.NUM_BITS_KEYS))
                )
        );
    }

    private NodeInfo requestPredecessor(NodeInfo node) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        //ServerSocket tempSocket = new ServerSocket(0);
        SSLServerSocket tempSocket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        FetchPredecessorMessage msg = new FetchPredecessorMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()));

        Future<NodeInfo> request = this.communicator.listenOnSocket(tempSocket);

        this.communicator.send(Utils.createClientSocket(node.address, node.port), msg);

        ConsoleLogger.log(Level.INFO, "Sent predecessor request for node at " + node.address + ":" + node.port);

        this.ongoingOperations.put(new PredecessorRequestOperationEntry(new SimpleNodeInfo(node)), request);

        return request.get();
    }

    public void handlePredecessorRequest(SimpleNodeInfo originNode) throws IOException, NoSuchAlgorithmException {
        PredecessorMessage msg;

        if(this.predecessor == null) {
            msg = new PredecessorMessage(new SimpleNodeInfo(this.nodeInfo), new NullNodeInfo());
        } else {
            msg = new PredecessorMessage(new SimpleNodeInfo(this.nodeInfo), new SimpleNodeInfo(this.predecessor));
        }

        SSLSocket targetSocket = (SSLSocket) SSLSocketFactory.getDefault().createSocket(originNode.address, originNode.port);

        ConsoleLogger.log(Level.WARNING, "Sending Predecessor info for node on port " + targetSocket.getPort());
        this.communicator.send(targetSocket, msg);

    }


    public void handleStatusCheck(SimpleNodeInfo originNode) throws IOException, NoSuchAlgorithmException {
        StatusCheckConfirmMessage msg = new StatusCheckConfirmMessage(new SimpleNodeInfo(this.nodeInfo));
        this.communicator.send(Utils.createClientSocket(originNode.address, originNode.port), msg);
    }

    private void startListening() throws IOException {
        //ServerSocket s = new ServerSocket(nodeInfo.port);
        SSLServerSocket serverSocket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(this.nodeInfo.port);
        System.out.println(Arrays.toString(serverSocket.getEnabledCipherSuites()));

        this.communicator = new Communicator(this, serverSocket);

        this.communicator.listen();
    }

    public void setSuccessor(NodeInfo successorInfo) {

        NodeInfo successor;
        if(successorInfo instanceof NullNodeInfo) {
            successor = this.nodeInfo;
        } else {
            successor = successorInfo;
        }

        this.successor = successor;
        this.fingerTable.put(1, successor);

        ConsoleLogger.log(Level.SEVERE, "My successor is now " + successor.id);
    }

    private void setPredecessor(NodeInfo predecessor) {
        this.predecessor = predecessor;

        if(predecessor != null) {
            ConsoleLogger.log(Level.SEVERE, "My predecessor is now " + predecessor.id);
        } else {
            ConsoleLogger.log(Level.SEVERE, "My predecessor is now null");
        }


    }

    public void concludeOperation(OperationEntry operation) {

        if(this.ongoingOperations.containsKey(operation)) {
            Future operationFuture = this.ongoingOperations.get(operation);
            if(!operationFuture.isDone()) {
                operationFuture.cancel(true);
            }
            this.ongoingOperations.remove(operation);
        }
    }

    private void bootstrapStabilizer() {
        this.threadPool.scheduleWithFixedDelay(() -> {
            try {
                stabilize();
            } catch (IOException | InterruptedException | NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }, 0, STABILIZATION_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void bootstrapFixFingers() {
        this.threadPool.scheduleWithFixedDelay(() -> {
            try {
                fixFingers();
            } catch (IOException | InterruptedException | NoSuchAlgorithmException | ExecutionException e) {
                e.printStackTrace();
            }
        }, 0, FIX_FINGER_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void bootstrapCheckPredecessor() {
        this.threadPool.scheduleWithFixedDelay(() -> {
            try {
                checkPredecessor();
            } catch (IOException | InterruptedException | NoSuchAlgorithmException | ExecutionException e) {
                e.printStackTrace();
            }
        }, 0, CHECK_PREDECESSOR_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public ScheduledExecutorService getThreadPool() {
        return threadPool;
    }

    public NodeInfo getNodeInfo() {
        return this.nodeInfo;
    }

    public CompletableFuture<NodeInfo> requestBackup(ReplicaIdentifier replicaId, byte[] fileContent) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        SSLServerSocket tempSocket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        BackupRequestMessage msg = new BackupRequestMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()), replicaId);

        NodeInfo targetNode = this.findSuccessor(replicaId.getHash());

        CompletableFuture<ChordMessage> request = this.communicator.async_listenOnSocket(tempSocket);

        this.communicator.send(Utils.createClientSocket(targetNode.address, targetNode.port), msg);

        ConsoleLogger.log(Level.SEVERE, "I want to save file with key " + replicaId);
        ConsoleLogger.log(Level.INFO, "Sent backup request for node at " + targetNode.address + ":" + targetNode.port);

        ChordMessage backupRequestResponse = request.get();

        if(backupRequestResponse instanceof BackupACKMessage){
            SimpleNodeInfo payloadTarget = ((NodeInfoMessage) backupRequestResponse).getNode();

            SSLServerSocket payloadResponseSocket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(0);
            payloadResponseSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

            BackupPayloadMessage payloadMsg = new BackupPayloadMessage(new SimpleNodeInfo(this.nodeInfo.address, this.nodeInfo.port), replicaId, fileContent);

            CompletableFuture<ChordMessage> payloadResponse = this.communicator.async_listenOnSocket(payloadResponseSocket);

            this.communicator.send(Utils.createClientSocket(payloadTarget.address, payloadTarget.port), payloadMsg);

            ChordMessage payloadResponseMessage = payloadResponse.get();

            if(!(payloadResponseMessage instanceof BackupConfirmMessage)){
                ConsoleLogger.log(Level.SEVERE, "Failed to store replica of file!");
            }
        }else if(backupRequestResponse instanceof BackupNACKMessage){
            ConsoleLogger.log(Level.WARNING, "No peer had enough space to store file!");
        }

        return new NodeInfo(((NodeInfoMessage) backupRequestResponse).getNode();
    }

    public void handleBackupRequest(BackupRequestMessage request) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        //CODE TO SAVE FILE
//        Path directory = FileManager.createDirectory("backup", Node.NODE_PATH);
//
//        FileManager.writeToFile(directory.resolve(fileId.toString()).toString(), data);
//
//        BackupConfirmMessage msg = new BackupConfirmMessage(new SimpleNodeInfo(this.nodeInfo));
//
//
//
//        this.communicator.send(Utils.createClientSocket(originNode.address, originNode.port), msg);

        if(new NodeInfo(request.getOriginNode()).id.equals(this.nodeInfo.id)){
            this.communicator.send(Utils.createClientSocket(request.getOriginNode().address, request.getOriginNode().port),
                    new BackupNACKMessage(request.getOriginNode(), request.getReplicaId()));
        }

        this.backupManager.storeReplica(request);

    }

    public State getState() {
        return this.state;
    }

    public Communicator getCommunicator() {
        return communicator;
    }

    public NodeInfo getSuccessor() {
        return this.successor;
    }
}
