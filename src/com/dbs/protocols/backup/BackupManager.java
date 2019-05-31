package com.dbs.protocols.backup;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.chord.Utils;
import com.dbs.filemanager.FileManager;
import com.dbs.network.messages.*;
import com.dbs.utils.ConsoleLogger;
import com.dbs.utils.Network;

import javax.net.ssl.SSLServerSocket;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import static com.dbs.chord.Node.REQUEST_TIMEOUT_MS;
import static com.dbs.chord.Utils.between;
import static java.util.logging.Level.*;

public class BackupManager {

    private final Node node;

    private final ConcurrentHashMap<FileIdentifier, Integer> desiredFileRepDegree;

    public BackupManager(Node node) {
        this.node = node;
        this.desiredFileRepDegree = new ConcurrentHashMap<>();

    }

    public String backup(String file, int repDegree) throws RemoteException {
        ConsoleLogger.log(INFO,"Starting backup");


        // Waits for *all* futures to complete and returns a list of results.
        // If *any* future completes exceptionally then the resulting future will also complete exceptionally.

        ReplicaIdentifier[] replicaIds;
        byte[] fileContent;
        try {
            FileIdentifier fileId = FileIdentifier.fromPath(file);
            replicaIds = FileManager.generateReplicaIds(fileId, repDegree);
            this.desiredFileRepDegree.put(fileId, repDegree);
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RemoteException("Could not generate file ids", e);
        }

        try {
            fileContent = FileManager.readFromFile(file);
        } catch (ExecutionException | InterruptedException | FileNotFoundException e) {
            throw new RemoteException("Could not read file contents", e);
        }

        ArrayList<CompletableFuture<NodeInfo>> futures = initBackupOperation(replicaIds, fileContent);

        // Wait until they are all done

        StringBuilder retMsg = new StringBuilder();


        handleBackupFutures(retMsg, futures);


        return retMsg.toString();
    }

    private ArrayList<CompletableFuture<NodeInfo>> initBackupOperation(ReplicaIdentifier[] replicaIds, byte[] fileContent) {

        ArrayList<CompletableFuture<NodeInfo>> futures = new ArrayList<>(replicaIds.length);

        for (int i = 0; i < replicaIds.length; i++) {
            try {
                if(this.node.getState().hasReplicaLocation(replicaIds[i])){
                    CompletableFuture<NodeInfo> future = new CompletableFuture<>();
                    future.complete(new NodeInfo(this.node.getState().getReplicasLocation().get(replicaIds[i])));
                    futures.add(i, future);
                }else {
                    CompletableFuture<NodeInfo> currRequest = this.node.requestBackup(replicaIds[i], fileContent);
                    futures.add(currRequest);
                }
            } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
                //continue
            }
        }

        return futures;

    }

    public void checkStoreReplica(BackupRequestMessage request) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        try {
            BackupResponseMessage msg;

            if(this.node.getState().hasFileToStore(request.getReplicaId())){
                msg = new BackupConfirmMessage(new SimpleNodeInfo(this.node.getNodeInfo()), request.getReplicaId());
                this.node.getState().addReplica(request.getReplicaId());
            }else{
                msg = new BackupACKMessage(new SimpleNodeInfo(this.node.getNodeInfo()), request.getReplicaId());
            }

            this.node.getState().setReplicaLocation(request.getReplicaId(), new SimpleNodeInfo(this.node.getNodeInfo()));
            this.node.getCommunicator().send(Utils.createClientSocket(request.getResponseSocketInfo().address, request.getResponseSocketInfo().port), msg);
        } catch (NoSpaceException e) {

            if(request.isOriginalRequest()){

                SSLServerSocket successorRequestSocket = Network.createServerSocket(0);
                successorRequestSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

                BackupRequestMessage msg = new BackupRequestMessage(new SimpleNodeInfo(this.node.getNodeInfo().address, successorRequestSocket.getLocalPort()), new SimpleNodeInfo(this.node.getNodeInfo()), request.getReplicaId(), false);

                CompletableFuture<ChordMessage> requestSuccessor = this.node.getCommunicator().async_listenOnSocket(successorRequestSocket);

                this.node.getCommunicator().send(Utils.createClientSocket(this.node.getSuccessor().address, this.node.getSuccessor().port), msg);

                ChordMessage successorResponse = requestSuccessor.get();

                if(successorResponse instanceof BackupConfirmMessage || successorResponse instanceof BackupACKMessage){
                    this.node.getState().setReplicaLocation(request.getReplicaId(), ((NodeInfoMessage) successorResponse).getNode());
                }

                this.node.getCommunicator().send(Utils.createClientSocket(request.getResponseSocketInfo().address, request.getResponseSocketInfo().port), successorResponse);

            }else {
                BackupRequestMessage msg = new BackupRequestMessage(request.getResponseSocketInfo(), request.getOriginNode(), request.getReplicaId(), false);
                this.node.getCommunicator().send(Utils.createClientSocket(this.node.getSuccessor().address, this.node.getSuccessor().port), msg);
            }


        }
    }

    public void storeReplica(BackupPayloadMessage backupPayloadMessage) throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException {
        try {
            if (!this.node.getState().addReplica(backupPayloadMessage.getReplicaId())) {
                Path directory = FileManager.getOrCreateDirectory("backup", Node.NODE_PATH);
                FileManager.writeToFile(directory.resolve(String.valueOf(backupPayloadMessage.getReplicaId().getFileId().hashCode())).toString(), backupPayloadMessage.getData());
            }

            BackupConfirmMessage msg = new BackupConfirmMessage(new SimpleNodeInfo(this.node.getNodeInfo()), backupPayloadMessage.getReplicaId());
            this.node.getCommunicator().send(Utils.createClientSocket(backupPayloadMessage.getResponseSocketInfo().address, backupPayloadMessage.getResponseSocketInfo().port), msg);

        }catch(NoSpaceException e){
            BackupNACKMessage msg = new BackupNACKMessage(new SimpleNodeInfo(this.node.getNodeInfo()), backupPayloadMessage.getReplicaId());
            this.node.getCommunicator().send(Utils.createClientSocket(backupPayloadMessage.getResponseSocketInfo().address, backupPayloadMessage.getResponseSocketInfo().port), msg);
        }
    }

    public void redistributeEligibleReplicas(NodeInfo otherNode) {

        this.node.getState().getReplicasLocation().forEach((replica, location) -> {
            if(between(replica.hash, this.node.getNodeInfo().id, otherNode.id)) {
                try {
                    ConsoleLogger.log(Level.INFO, "Sending replica " + replica.getHash() + " to new predecessor " + otherNode.id);

                    UpdateReplicaLocationMessage updateReplicaLocationInPredecessorMsg = new UpdateReplicaLocationMessage(replica, location);

                    this.node.getCommunicator().send(Utils.createClientSocket(otherNode.address, otherNode.port), updateReplicaLocationInPredecessorMsg);

                    this.node.getState().removeReplicaLocation(replica);

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        });
    }

    public String state() throws RemoteException {
        return this.node.getState().toString();
    }



    public void handleBackupFutures(StringBuilder retMsg, ArrayList<CompletableFuture<NodeInfo>> futures) {
        for (int i = 0; i < futures.size(); i++) {

            CompletableFuture<NodeInfo> future = futures.get(i);

            try {

                BigInteger id = future.get().id;

                retMsg.append("Successfully saved replica " + i + " in node " + id + ".\n");

            } catch (InterruptedException e) {
                e.printStackTrace();
                continue;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof NoSpaceException) {
                    retMsg.append("Could not store replica " + i + " of file. Not enough space in any peer.\n");
                } else {
                    e.printStackTrace();
                }
                continue;
            }

        }
    }




    public Integer getDesiredFileRepDegreeOfFile(FileIdentifier fileId) {
        return this.desiredFileRepDegree.get(fileId);
    }

    public ConcurrentHashMap<FileIdentifier, Integer> getDesiredFileRepDegrees() {
        return this.desiredFileRepDegree;
    }

    public CompletableFuture<NodeInfo> requestBackup(ReplicaIdentifier replicaId, byte[] fileContent, NodeInfo targetNode) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        SSLServerSocket tempSocket = Network.createServerSocket(0);
        tempSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

        BackupRequestMessage msg = new BackupRequestMessage(
                new SimpleNodeInfo(this.node.getNodeInfo().address, tempSocket.getLocalPort()),
                new SimpleNodeInfo(this.node.getNodeInfo().address, this.node.getNodeInfo().port),
                replicaId,
                true);

        CompletableFuture<ChordMessage> request = this.node.getCommunicator().async_listenOnSocket(tempSocket);

        this.node.getCommunicator().send(Utils.createClientSocket(targetNode.address, targetNode.port), msg);

        ConsoleLogger.log(Level.SEVERE, "I want to save file with key " + replicaId.getHash());
        ConsoleLogger.log(Level.SEVERE, "Sent backup request for node at " + targetNode.address + ":" + targetNode.port);


        ChordMessage backupRequestResponse = request.get();

        CompletableFuture<NodeInfo> ret = new CompletableFuture<>();


        if (backupRequestResponse instanceof BackupACKMessage) {
            SimpleNodeInfo payloadTarget = ((NodeInfoMessage) backupRequestResponse).getNode();

            SSLServerSocket payloadResponseSocket = Network.createServerSocket(0);
            payloadResponseSocket.setSoTimeout(REQUEST_TIMEOUT_MS);

            BackupPayloadMessage payloadMsg = new BackupPayloadMessage(new SimpleNodeInfo(this.node.getNodeInfo().address, payloadResponseSocket.getLocalPort()), replicaId, fileContent);

            CompletableFuture<ChordMessage> payloadResponse = this.node.getCommunicator().async_listenOnSocket(payloadResponseSocket);

            this.node.getCommunicator().send(Utils.createClientSocket(payloadTarget.address, payloadTarget.port), payloadMsg);

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

    public CompletableFuture<NodeInfo> requestBackup(ReplicaIdentifier replicaId, byte[] fileContent) throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException {
        NodeInfo targetNode = this.node.findSuccessor(replicaId.getHash());

        return requestBackup(replicaId, fileContent, targetNode);
    }

    public void handleBackupRequest(BackupRequestMessage request) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        if (new NodeInfo(request.getOriginNode()).id.equals(this.node.getNodeInfo().id) && !request.isOriginalRequest()) {
            this.node.getCommunicator().send(Utils.createClientSocket(request.getResponseSocketInfo().address, request.getResponseSocketInfo().port),
                    new BackupNACKMessage(request.getResponseSocketInfo(), request.getReplicaId()));

            return;
        }
        this.checkStoreReplica(request);
    }
}
