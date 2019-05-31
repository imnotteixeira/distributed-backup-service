package com.dbs.chord;

import com.dbs.protocols.DistributedBackupServiceAdapter;
import com.dbs.protocols.backup.BackupManager;
import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.network.Communicator;
import com.dbs.network.NullNodeInfo;
import com.dbs.network.messages.*;
import com.dbs.protocols.delete.DeleteManager;
import com.dbs.protocols.reclaim.ReclaimManager;
import com.dbs.protocols.restore.RestoreManager;
import com.dbs.utils.ConsoleLogger;
import com.dbs.utils.Network;
import com.dbs.utils.State;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.NavigableSet;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.dbs.chord.Utils.between;
import static java.util.logging.Level.SEVERE;

public class Node implements Chord {

    public static final int INITIAL_SPACE_LIMIT_BYTES = (int) (600 * 10e3);
    public static final int REQUEST_TIMEOUT_MS = 5000;
    private static final int THREAD_POOL_SIZE = 150;
    private static final int STABILIZATION_INTERVAL_MS = 200;
    private static final int FIX_FINGER_INTERVAL_MS = 200;
    private static final int CHECK_PREDECESSOR_INTERVAL_MS = 200;
    public static String NODE_PATH;

    private DistributedBackupServiceAdapter dbsAdapter;

    private BackupManager backupManager;
    private RestoreManager restoreManager;
    private DeleteManager deleteManager;
    private ReclaimManager reclaimManager;


    private ScheduledExecutorService threadPool;
    private Communicator communicator;

    private ConcurrentSkipListMap<Integer, NodeInfo> fingerTable;

    private NodeInfo nodeInfo;
    private NodeInfo predecessor;
    private NodeInfo successor;

    private int nextFinger = 0;
    private State state;


    public Node(NodeInfo nodeInfo) throws IOException, NoSuchAlgorithmException {

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

    private void initNode(NodeInfo nodeInfo) throws IOException, NoSuchAlgorithmException {
        this.nodeInfo = nodeInfo;

        ConsoleLogger.log(Level.SEVERE, "My ID: " + nodeInfo.id);


        final String nodeAP = this.nodeInfo.getAccessPoint();

        Node.NODE_PATH = nodeAP;


        this.backupManager = new BackupManager(this);
        this.restoreManager = new RestoreManager(this);
        this.deleteManager = new DeleteManager(this);
        this.reclaimManager = new ReclaimManager(this);

        this.dbsAdapter = new DistributedBackupServiceAdapter(this);


        this.threadPool = Executors.newScheduledThreadPool(THREAD_POOL_SIZE);

        this.fingerTable = new ConcurrentSkipListMap<>();

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
     *
     * @param key - key to search
     * @return NodeInfo of successor's node
     */
    @Override
    public NodeInfo findSuccessor(BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        //if this is the starter node, it is responsible for any key for now
        if (this.successor.id.equals(this.nodeInfo.id)) {
            return this.nodeInfo;
        }

        //if this node currently has no predecessor and the key equals this node's id, this is the responsible node
        if (this.predecessor == null && key.equals(this.nodeInfo.id)) {

            return this.nodeInfo;
        }

        //if there is a predecessor and the key is between predecessor and current node, current node is responsible
        if (this.predecessor != null && between(key, predecessor.id, this.nodeInfo.id)) {

            return this.nodeInfo;
        }

        //if key > node && key <= successor
        if (between(key, this.nodeInfo.id, this.successor.id) || key.equals(this.successor.id)) {

            return this.successor;
        } else {

            NodeInfo nextNode = closestPrecedingNode(key);
            return this.requestSuccessor(nextNode, key);
        }

    }

    private NodeInfo closestPrecedingNode(BigInteger key) {
        NavigableSet<Integer> fingers = fingerTable.descendingKeySet();

        for (Integer finger : fingers) {

            if (between(this.fingerTable.get(finger).id, this.nodeInfo.id, key)) {
                return this.fingerTable.get(finger);
            }
        }

        return this.nodeInfo;
    }

    /**
     * Inserts a Future in the ongoingOperations Map and waits for the future to resolve with given timeout
     * Sends a TCP message to target node to find the successor of key
     * When this node receives a msg regarding this request, the promise is resovled with the nodeinfo attached and this function returns
     *
     * @param targetNode - node to request
     * @param key        - key to find
     * @return
     */
    private NodeInfo requestSuccessor(NodeInfo targetNode, BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        //ServerSocket tempSocket = new ServerSocket(0);
        SSLServerSocket tempSocket = Network.createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        FindSuccessorMessage msg = new FindSuccessorMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()), key);

        SSLSocket targetSocket = (SSLSocket) SSLSocketFactory.getDefault().createSocket(targetNode.address, targetNode.port);

        Future<NodeInfo> request = this.communicator.listenOnSocket(tempSocket);
        this.communicator.send(targetSocket, msg);

        return request.get();
    }

    /**
     * If this node is the successor or the predecessor of successor, answers back to temp socket of origin node
     * Ohterwise, forwards a TCP message to target node to find the successor of key
     *
     * @param responseSocketInfo - socket info to send the response
     * @param key                - key to find
     * @return
     */
    public void handleSuccessorRequest(SimpleNodeInfo responseSocketInfo, BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {


        NodeInfo successor = this.findSuccessor(key);
        NodeInfo asker = new NodeInfo(responseSocketInfo);

        //If we already know the actual successor
        if (successor.equals(this.nodeInfo) || successor.equals(this.successor)) {
            SuccessorMessage msg = new SuccessorMessage(key, new SimpleNodeInfo(successor));
            this.communicator.send(Utils.createClientSocket(asker.address, asker.port), msg);
        } else { //else propagate to other target, based on fingerTable
            FindSuccessorMessage msg = new FindSuccessorMessage(responseSocketInfo, key);
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

        NotifySuccessorMessage msg = new NotifySuccessorMessage(new SimpleNodeInfo(this.nodeInfo));

        this.communicator.send(Utils.createClientSocket(successor.address, successor.port), msg);

    }

    /**
     * potentialPredecessor thinks it might be this node's predecessor
     *
     * @param potentialPredecessorInfo
     */
    @Override
    public void handlePredecessorNotification(SimpleNodeInfo potentialPredecessorInfo) throws IOException, NoSuchAlgorithmException {
        NodeInfo potentialPredecessor = new NodeInfo(potentialPredecessorInfo);

        if (this.predecessor == null || this.predecessor.id.equals(this.nodeInfo.id) || between(potentialPredecessor.id, this.predecessor.id, this.nodeInfo.id)) {
            this.setPredecessor(potentialPredecessor);

            this.backupManager.redistributeEligibleReplicas(potentialPredecessor);
        }

    }

    /**
     * Called periodically. verifies this node's immediate successor, and tells the successor about itself.
     */
    @Override
    public void stabilize() throws IOException, InterruptedException, NoSuchAlgorithmException {
        NodeInfo x;
        if (this.successor.id.equals(this.nodeInfo.id)) {

            if (this.predecessor == null) return;

            x = this.predecessor;

        } else {
            try {
                x = requestPredecessor(this.successor);
            } catch (ExecutionException | SocketException e) {
                //if this request fails, it means my successor prolly is offline, must update stuffs
                this.handleSuccessorFail();
                return;
            }
        }


        if (!(x instanceof NullNodeInfo)) {
            if (!x.id.equals(this.successor.id) && between(x.id, this.nodeInfo.id, this.successor.id)) {
                this.setSuccessor(x);
            } else if (this.successor.id.equals(this.nodeInfo.id) && this.predecessor != null) { // when I have a predecessor (newly joined node) but it should be my successor
                this.setSuccessor(this.predecessor);
            }
        }


        try {
            this.notify(this.successor);
        } catch (Exception e) {
            //Successor was removed before sending notification
        }

    }

    @Override
    public void handleSuccessorFail() {
        ConsoleLogger.log(SEVERE, "Successor offline - Fault tolerance not implemented");
    }


    @Override
    public synchronized void checkPredecessor() throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        if (this.predecessor == null || this.predecessor.id.equals(this.nodeInfo.id)) return;

        SSLServerSocket tempSocket = Network.createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        StatusCheckMessage msg = new StatusCheckMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()));

        try {

            Future<NodeInfo> request = this.communicator.listenOnSocket(tempSocket);

            this.communicator.send(Utils.createClientSocket(this.predecessor.address, this.predecessor.port), msg);

            request.get();

        } catch (Exception e) {
            ConsoleLogger.log(SEVERE, "Predecessor went offline!");
            if (this.predecessor.id.equals(this.successor.id)) {
                this.setSuccessor(new NullNodeInfo());
            }
            this.setPredecessor(null);
        }
    }

    @Override
    public void fixFingers() throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException {
        this.nextFinger += 1;
        if (this.nextFinger > Chord.NUM_BITS_KEYS) {
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
        SSLServerSocket tempSocket = Network.createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        FetchPredecessorMessage msg = new FetchPredecessorMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()));

        Future<NodeInfo> request = this.communicator.listenOnSocket(tempSocket);

        this.communicator.send(Utils.createClientSocket(node.address, node.port), msg);

        return request.get();
    }

    public void handlePredecessorRequest(SimpleNodeInfo responseSocketInfo) throws IOException, NoSuchAlgorithmException {
        PredecessorMessage msg;

        if (this.predecessor == null) {
            msg = new PredecessorMessage(new SimpleNodeInfo(this.nodeInfo), new NullNodeInfo());
        } else {
            msg = new PredecessorMessage(new SimpleNodeInfo(this.nodeInfo), new SimpleNodeInfo(this.predecessor));
        }

        SSLSocket targetSocket = (SSLSocket) SSLSocketFactory.getDefault().createSocket(responseSocketInfo.address, responseSocketInfo.port);

        this.communicator.send(targetSocket, msg);

    }


    public void handleStatusCheck(SimpleNodeInfo responseSocketInfo) throws IOException, NoSuchAlgorithmException {
        StatusCheckConfirmMessage msg = new StatusCheckConfirmMessage(new SimpleNodeInfo(this.nodeInfo));
        this.communicator.send(Utils.createClientSocket(responseSocketInfo.address, responseSocketInfo.port), msg);
    }

    private void startListening() throws IOException, NoSuchAlgorithmException {
        //ServerSocket s = new ServerSocket(nodeInfo.port);
        SSLServerSocket serverSocket = Network.createServerSocket(this.nodeInfo.port);

        this.communicator = new Communicator(this, serverSocket);

        this.communicator.listen();
    }

    private void setPredecessor(NodeInfo predecessor) {
        this.predecessor = predecessor;

        if (predecessor != null) {
            ConsoleLogger.log(SEVERE, "My predecessor is now " + predecessor.id);
        } else {
            ConsoleLogger.log(SEVERE, "My predecessor is now null");
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
        return this.backupManager.requestBackup(replicaId, fileContent);
    }

    public void handleBackupRequest(BackupRequestMessage request) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        this.backupManager.handleBackupRequest(request);
    }

    public void handleBackupPayload(BackupPayloadMessage backupPayloadMessage) throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException {
        this.backupManager.storeReplica(backupPayloadMessage);
    }

    public CompletableFuture<NodeInfo> requestRestore(ReplicaIdentifier replicaId) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        return this.restoreManager.requestRestore(replicaId);
    }

    public void handleRestoreRequest(RestoreRequestMessage message) throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException {
        this.restoreManager.handleRestoreRequest(message);
    }

    public void handleReplicaDeletion(DeleteReplicaMessage deleteReplicaMessage) {
        this.deleteManager.deleteReplica(deleteReplicaMessage);
    }

    public CompletableFuture<NodeInfo> delete(ReplicaIdentifier replicaId) {
        return this.deleteManager.deleteReplica(replicaId);
    }

    public void updateReplicaLocation(ReplicaIdentifier replicaId, SimpleNodeInfo node) {
        this.getState().setReplicaLocation(replicaId, node);
    }

    public BackupManager getBackupManager() {
        return this.backupManager;
    }

    public DeleteManager getDeleteManager() {
        return deleteManager;
    }

    public ReclaimManager getReclaimManager() {
        return reclaimManager;
    }

    public RestoreManager getRestoreManager() {
        return this.restoreManager;
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

    public void setSuccessor(NodeInfo successorInfo) {

        NodeInfo successor;
        if (successorInfo instanceof NullNodeInfo) {
            successor = this.nodeInfo;
        } else {
            successor = successorInfo;
        }

        this.successor = successor;
        this.fingerTable.put(1, successor);

        ConsoleLogger.log(SEVERE, "My successor is now " + successor.id);
    }


}
