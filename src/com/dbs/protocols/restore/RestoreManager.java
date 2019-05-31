package com.dbs.protocols.restore;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.chord.Utils;
import com.dbs.filemanager.FileManager;
import com.dbs.network.messages.*;
import com.dbs.protocols.backup.FileIdentifier;
import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.utils.ConsoleLogger;
import com.dbs.utils.Network;

import javax.net.ssl.SSLServerSocket;
import java.io.IOException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import static com.dbs.chord.Node.NODE_PATH;
import static com.dbs.chord.Node.REQUEST_TIMEOUT_MS;
import static com.dbs.chord.Utils.createClientSocket;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

public class RestoreManager {

    private final Node node;

    public RestoreManager(Node node) {
        this.node = node;

    }


    public String restore(String file) throws RemoteException {
        ConsoleLogger.log(INFO,"Starting restore");
        try {
            FileIdentifier fileId = FileIdentifier.fromPath(file);
            if (this.node.getBackupManager().getDesiredFileRepDegreeOfFile(fileId) == null) {
                ConsoleLogger.log(SEVERE, "File is not backed up");
                return "File is not backed up";
            }
            if (this.node.getState().hasFile(fileId)) {
                ConsoleLogger.log(SEVERE, "I have the file.");
                this.restoreFromOwnStorage(fileId);
                return "Restored from own storage";
            } else {
                ReplicaIdentifier[] replicaIds;
                replicaIds = FileManager.generateReplicaIds(fileId, this.node.getBackupManager().getDesiredFileRepDegreeOfFile(fileId));
                for (ReplicaIdentifier r: replicaIds) {
                    try {
                        NodeInfo res = this.node.requestRestore(r).get();
                        return "Restored file " + fileId.getFileName() + " from node at " + res.address + ":" + res.port;
                    } catch (ExecutionException | InterruptedException e) {
                        ConsoleLogger.log(Level.SEVERE, e.getMessage());
                    }
                }
            }
        } catch (IOException | NoSuchAlgorithmException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return "Failed to restore file.";
    }

    public CompletableFuture<NodeInfo> requestRestore(ReplicaIdentifier replicaId) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        CompletableFuture<NodeInfo> ret = new CompletableFuture<>();

        SSLServerSocket tempSocket = Network.createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        SimpleNodeInfo thisNode = new SimpleNodeInfo(this.node.getNodeInfo().address, tempSocket.getLocalPort());

        RestoreRequestMessage msg = new RestoreRequestMessage(thisNode, thisNode, replicaId);

        NodeInfo targetNode = this.node.findSuccessor(replicaId.getHash());

        CompletableFuture<ChordMessage> request = this.node.getCommunicator().async_listenOnSocket(tempSocket);

        this.node.getCommunicator().send(Utils.createClientSocket(targetNode.address, targetNode.port), msg);

        ConsoleLogger.log(SEVERE, "I want to restore file with key " + replicaId);
        ConsoleLogger.log(SEVERE, "Sent restore request for node at " + targetNode.address + ":" + targetNode.port);

        ChordMessage restoreRequestResponse = request.get();

        if(restoreRequestResponse instanceof NotFoundMessage){
            ret.completeExceptionally(new Exception("Could not find file to restore!"));
            return ret;
        }

        if (restoreRequestResponse instanceof RestorePayloadMessage) {
            RestorePayloadMessage message = (RestorePayloadMessage) restoreRequestResponse;
            this.storeRestorePayload(message);
        }

        ret.complete(new NodeInfo(((RestorePayloadMessage) restoreRequestResponse).getOriginNode()));

        return ret;
    }

    private void storeRestorePayload(RestorePayloadMessage message) throws IOException, ExecutionException, InterruptedException {

        Path directory = FileManager.getOrCreateDirectory("restored", NODE_PATH);

        String fileName = String.valueOf(message.getReplicaId().getFileId().getFileName());

        FileManager.writeToFile(directory.resolve(fileName).toString(), message.getData());


    }


    public void handleRestoreRequest(RestoreRequestMessage message) throws InterruptedException, ExecutionException, IOException, NoSuchAlgorithmException {
        ConsoleLogger.log(SEVERE, "Received restore request");
        final ReplicaIdentifier replicaId = message.getReplicaId();
        if (this.node.getState().hasReplica(replicaId)) {
            ConsoleLogger.log(SEVERE, "I have the file");
            String fileName = String.valueOf(replicaId.getFileId().hashCode());
            Path directory = FileManager.getOrCreateDirectory("backup", NODE_PATH);
            byte[] data = FileManager.readFromFile(directory.resolve(fileName).toString());
            this.node.getCommunicator().send(createClientSocket(message.getRequestSocketInfo().address, message.getRequestSocketInfo().port),
                    new RestorePayloadMessage(new SimpleNodeInfo(this.node.getNodeInfo()), replicaId, data));
            ConsoleLogger.log(SEVERE, "Sent it over");
        } else {
            ConsoleLogger.log(SEVERE, "I know where the file is");
            SimpleNodeInfo replicaLocation = this.node.getState().getReplicaLocation(replicaId);
            if (replicaLocation != null) {
                RestoreRequestMessage msg = new RestoreRequestMessage(message.getRequestSocketInfo(), new SimpleNodeInfo(this.node.getNodeInfo()), message.getReplicaId());
                this.node.getCommunicator().send(Utils.createClientSocket(replicaLocation.address, replicaLocation.port), msg);
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


}
