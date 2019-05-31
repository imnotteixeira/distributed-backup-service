package com.dbs.chord;

import com.dbs.backup.BackupManager;
import com.dbs.backup.FileIdentifier;
import com.dbs.backup.NoSpaceException;
import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.operations.OperationEntry;
import com.dbs.chord.operations.PredecessorRequestOperationEntry;
import com.dbs.chord.operations.SuccessorRequestOperationEntry;
import com.dbs.filemanager.FileManager;
import com.dbs.network.Communicator;
import com.dbs.network.NullNodeInfo;
import com.dbs.network.messages.*;
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
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.NavigableSet;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.dbs.chord.Utils.between;
import static com.dbs.chord.Utils.createClientSocket;
import static java.util.logging.Level.SEVERE;

public class Node implements Chord {

    public static final int INITIAL_SPACE_LIMIT_BYTES = (int) (600 * 10e3);
    public static final int REQUEST_TIMEOUT_MS = 5000;
    private static final int THREAD_POOL_SIZE = 150;
    private static final int STABILIZATION_INTERVAL_MS = 200;
    private static final int FIX_FINGER_INTERVAL_MS = 200;
    private static final int CHECK_PREDECESSOR_INTERVAL_MS = 200;
    public static String NODE_PATH;

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


        this.ongoingOperations.put(new SuccessorRequestOperationEntry(key), request);

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

        this.ongoingOperations.put(new PredecessorRequestOperationEntry(new SimpleNodeInfo(node)), request);

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

    public void concludeOperation(OperationEntry operation) {

        if (this.ongoingOperations.containsKey(operation)) {
            Future operationFuture = this.ongoingOperations.get(operation);
            if (!operationFuture.isDone()) {
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

    public CompletableFuture<NodeInfo> requestBackup(ReplicaIdentifier replicaId, byte[] fileContent, NodeInfo targetNode) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        SSLServerSocket tempSocket = Network.createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        BackupRequestMessage msg = new BackupRequestMessage(
                new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()),
                new SimpleNodeInfo(this.nodeInfo.address, this.nodeInfo.port),
                replicaId,
                true);

        CompletableFuture<ChordMessage> request = this.communicator.async_listenOnSocket(tempSocket);

        this.communicator.send(Utils.createClientSocket(targetNode.address, targetNode.port), msg);

        ConsoleLogger.log(Level.SEVERE, "I want to save file with key " + replicaId.getHash());
        ConsoleLogger.log(Level.SEVERE, "Sent backup request for node at " + targetNode.address + ":" + targetNode.port);


        ChordMessage backupRequestResponse = request.get();

        CompletableFuture<NodeInfo> ret = new CompletableFuture<>();


        if (backupRequestResponse instanceof BackupACKMessage) {
            SimpleNodeInfo payloadTarget = ((NodeInfoMessage) backupRequestResponse).getNode();

            SSLServerSocket payloadResponseSocket = Network.createServerSocket(0);
            payloadResponseSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

            BackupPayloadMessage payloadMsg = new BackupPayloadMessage(new SimpleNodeInfo(this.nodeInfo.address, payloadResponseSocket.getLocalPort()), replicaId, fileContent);

            CompletableFuture<ChordMessage> payloadResponse = this.communicator.async_listenOnSocket(payloadResponseSocket);

            this.communicator.send(Utils.createClientSocket(payloadTarget.address, payloadTarget.port), payloadMsg);

            ChordMessage payloadResponseMessage = payloadResponse.get();


            if (payloadResponseMessage instanceof BackupNACKMessage) {
                ConsoleLogger.log(SEVERE, "Failed to store replica of file!");
                ret.complete(new NodeInfo(((NodeInfoMessage) backupRequestResponse).getNode()));
            } else if (payloadResponseMessage instanceof BackupConfirmMessage) {
                ret.complete(new NodeInfo(((NodeInfoMessage) payloadResponseMessage).getNode()));
            }
        } else if (backupRequestResponse instanceof BackupNACKMessage) {

            ConsoleLogger.log(SEVERE, "No peer had enough space to store file!");

            ret.completeExceptionally(new NoSpaceException());
        } else if (backupRequestResponse instanceof BackupConfirmMessage) {
            ret.complete(new NodeInfo(((NodeInfoMessage) backupRequestResponse).getNode()));
        } else {
            ret.completeExceptionally(new Exception("Received non-supported message answering to backup request"));
        }

        return ret;

    }

    public CompletableFuture<NodeInfo> requestBackup(ReplicaIdentifier replicaId, byte[] fileContent) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        NodeInfo targetNode = this.findSuccessor(replicaId.getHash());

        return requestBackup(replicaId, fileContent, targetNode);
    }

    public void handleBackupRequest(BackupRequestMessage request) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        if (new NodeInfo(request.getOriginNode()).id.equals(this.nodeInfo.id) && request.isOriginalRequest() == false) {
            this.communicator.send(Utils.createClientSocket(request.getResponseSocketInfo().address, request.getResponseSocketInfo().port),
                    new BackupNACKMessage(request.getResponseSocketInfo(), request.getReplicaId()));

            return;
        }


        this.backupManager.checkStoreReplica(request);

    }

    public void handleBackupPayload(BackupPayloadMessage backupPayloadMessage) throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException {
        this.backupManager.storeReplica(backupPayloadMessage);
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

    public CompletableFuture<NodeInfo> requestRestore(ReplicaIdentifier replicaId) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        CompletableFuture<NodeInfo> ret = new CompletableFuture<>();

        SSLServerSocket tempSocket = Network.createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        SimpleNodeInfo thisNode = new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort());

        RestoreRequestMessage msg = new RestoreRequestMessage(thisNode, thisNode, replicaId);

        NodeInfo targetNode = this.findSuccessor(replicaId.getHash());

        CompletableFuture<ChordMessage> request = this.communicator.async_listenOnSocket(tempSocket);

        this.communicator.send(Utils.createClientSocket(targetNode.address, targetNode.port), msg);

        ConsoleLogger.log(SEVERE, "I want to restore file with key " + replicaId);
        ConsoleLogger.log(SEVERE, "Sent restore request for node at " + targetNode.address + ":" + targetNode.port);

        ChordMessage restoreRequestResponse = request.get();

        if(restoreRequestResponse instanceof NotFoundMessage){
            ret.completeExceptionally(new Exception("Could not find file to restore!"));
            return ret;
        }

        if (restoreRequestResponse instanceof RestorePayloadMessage) {
            RestorePayloadMessage message = (RestorePayloadMessage) restoreRequestResponse;
            storeRestorePayload(message);
        }

        ret.complete(new NodeInfo(((RestorePayloadMessage) restoreRequestResponse).getOriginNode()));

        return ret;
    }

    private void storeRestorePayload(RestorePayloadMessage message) throws IOException, ExecutionException, InterruptedException {

        Path directory = FileManager.getOrCreateDirectory("restored", Node.NODE_PATH);

        String fileName = String.valueOf(message.getReplicaId().getFileId().getFileName());

        FileManager.writeToFile(directory.resolve(fileName).toString(), message.getData());


    }

    public void handleRestoreRequest(RestoreRequestMessage message) throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException {
        ConsoleLogger.log(SEVERE, "Received restore request");
        final ReplicaIdentifier replicaId = message.getReplicaId();
        if (this.state.hasReplica(replicaId)) {
            ConsoleLogger.log(SEVERE, "I have the file");
            String fileName = String.valueOf(replicaId.getFileId().hashCode());
            Path directory = FileManager.getOrCreateDirectory("backup", NODE_PATH);
            byte[] data = FileManager.readFromFile(directory.resolve(fileName).toString());
            this.communicator.send(createClientSocket(message.getRequestSocketInfo().address, message.getRequestSocketInfo().port),
                    new RestorePayloadMessage(new SimpleNodeInfo(this.getNodeInfo()), replicaId, data));
            ConsoleLogger.log(SEVERE, "Sent it over");
        } else {
            ConsoleLogger.log(SEVERE, "I know where the file is");
            SimpleNodeInfo replicaLocation = this.state.getReplicaLocation(replicaId);
            if (replicaLocation != null) {
                RestoreRequestMessage msg = new RestoreRequestMessage(message.getRequestSocketInfo(), new SimpleNodeInfo(this.nodeInfo), message.getReplicaId());
                this.communicator.send(Utils.createClientSocket(replicaLocation.address, replicaLocation.port), msg);
            }
        }
    }

    public void restoreFromOwnStorage(FileIdentifier fileId) throws IOException, ExecutionException, InterruptedException {
        Path directory = FileManager.getOrCreateDirectory("backup", NODE_PATH);
        String fileName = String.valueOf(fileId.hashCode());
        byte[] data = FileManager.readFromFile(directory.resolve(fileName).toString());
        directory = FileManager.getOrCreateDirectory("restored", NODE_PATH);
        FileManager.writeToFile(directory.resolve(fileId.getFileName()).toString(), data);
    }

    public void handleReplicaDeletion(DeleteReplicaMessage deleteReplicaMessage) {
        this.backupManager.deleteReplica(deleteReplicaMessage);
    }

    public CompletableFuture<NodeInfo> delete(ReplicaIdentifier replicaId) {

        CompletableFuture<NodeInfo> result = new CompletableFuture<NodeInfo>();

        try {

            SSLServerSocket tempSocket = Network.createServerSocket(0);
            tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

            NodeInfo targetNode = this.findSuccessor(replicaId.getHash());

            CompletableFuture<ChordMessage> future = this.communicator.async_listenOnSocket(tempSocket);

            DeleteReplicaMessage msg = new DeleteReplicaMessage(new SimpleNodeInfo(this.nodeInfo.address, tempSocket.getLocalPort()), replicaId);

            this.communicator.send(Utils.createClientSocket(targetNode.address, targetNode.port), msg);

            ChordMessage response = future.get();


            if(response instanceof NotFoundMessage){
                result.completeExceptionally(new Exception("Could not find file to delete!"));
                return result;
            }

            result.complete(new NodeInfo(((DeleteConfirmationMessage) response).getNode()));

        } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
            result.completeExceptionally(new Exception("Could not delete file. Maybe it is not in the network"));
        }

        return result;
    }

    public void updateReplicaLocation(ReplicaIdentifier replicaId, SimpleNodeInfo node) {
        this.getState().setReplicaLocation(replicaId, node);
    }
}
