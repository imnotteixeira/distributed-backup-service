package com.dbs.backup;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.chord.Utils;
import com.dbs.filemanager.FileManager;
import com.dbs.network.messages.*;
import com.dbs.utils.ConsoleLogger;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import static com.dbs.chord.Node.REQUEST_TIMEOUT_MS;
import static java.util.logging.Level.*;

public class BackupManager implements BackupService {

    private final Node node;

    public BackupManager(Node node) {
        this.node = node;

        BackupService service = null;
        try {
            service = (BackupService) UnicastRemoteObject.exportObject(this, 0);
            LocateRegistry.getRegistry().rebind(this.node.getNodeInfo().getAccessPoint(), service);
            ConsoleLogger.log(WARNING, "Found existing RMI registry");
        } catch (Exception e) {
            try {
                LocateRegistry.createRegistry(1099).rebind(this.node.getNodeInfo().getAccessPoint(), service);
                ConsoleLogger.log(WARNING, "Created new RMI registry");
            } catch (Exception exc) {
                exc.printStackTrace();
            }
        }
    }

    @Override
    public String backup(String file) throws RemoteException {
        ConsoleLogger.log(INFO,"Starting backup");


        // Waits for *all* futures to complete and returns a list of results.
        // If *any* future completes exceptionally then the resulting future will also complete exceptionally.

        ReplicaIdentifier[] replicaIds;
        byte[] fileContent;
        try {
            replicaIds = FileManager.generateReplicaIds(file, Node.REPLICATION_DEGREE);
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

    private ArrayList<CompletableFuture<NodeInfo>> initBackupOperation(ReplicaIdentifier[] fileIds, byte[] fileContent) {

        ArrayList<CompletableFuture<NodeInfo>> futures = new ArrayList<>(fileIds.length);

        for (int i = 0; i < fileIds.length; i++) {
            try {
                CompletableFuture<NodeInfo> currRequest = this.node.requestBackup(fileIds[i], fileContent);
                futures.add(i,currRequest);
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
                System.out.println("I already have the file!");
                msg = new BackupConfirmMessage(new SimpleNodeInfo(this.node.getNodeInfo()), request.getReplicaId());
                this.node.getState().addReplica(request.getReplicaId());
            }else{
                System.out.println("I Dont have the file yet, but you can send!");
                msg = new BackupACKMessage(new SimpleNodeInfo(this.node.getNodeInfo()), request.getReplicaId());
            }

            this.node.getState().setReplicaLocation(request.getReplicaId(), new SimpleNodeInfo(this.node.getNodeInfo()));
            System.out.println("Sending the above response to " + request.getResponseSocketInfo().address + ":" + request.getResponseSocketInfo().port);
            this.node.getCommunicator().send(Utils.createClientSocket(request.getResponseSocketInfo().address, request.getResponseSocketInfo().port), msg);
        } catch (NoSpaceException e) {

            if(request.isOriginalRequest()){

                SSLServerSocket successorRequestSocket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(0);
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
            System.out.println("answering to "+ backupPayloadMessage.getResponseSocketInfo().address + ":" + backupPayloadMessage.getResponseSocketInfo().port + " - thanks for the file!");
            this.node.getCommunicator().send(Utils.createClientSocket(backupPayloadMessage.getResponseSocketInfo().address, backupPayloadMessage.getResponseSocketInfo().port), msg);

        }catch(NoSpaceException e){
            BackupNACKMessage msg = new BackupNACKMessage(new SimpleNodeInfo(this.node.getNodeInfo()), backupPayloadMessage.getReplicaId());
            System.out.println("Could not store replica of "+ backupPayloadMessage.getResponseSocketInfo().address + ":" + backupPayloadMessage.getResponseSocketInfo().port + " after all");
            this.node.getCommunicator().send(Utils.createClientSocket(backupPayloadMessage.getResponseSocketInfo().address, backupPayloadMessage.getResponseSocketInfo().port), msg);
        }
    }

    public void redistributeEligibleReplicas(NodeInfo otherNode) {

        this.node.getState().getReplicasLocation().forEach((replica, location) -> {
            if(replica.hash.compareTo(otherNode.id) < 0) {
                try {
                    this.node.requestBackup(
                            replica,
                            FileManager.readFromFile(
                                    Paths.get(
                                            Node.NODE_PATH,
                                            "backup",
                                            String.valueOf(replica.getFileId().hashCode())).toString()
                            ),
                            new NodeInfo(location)
                    );


                } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
    }


    @Override
    public String restore(String file) throws RemoteException {
        ConsoleLogger.log(INFO,"Starting restore");

        try {
            FileIdentifier fileId = FileIdentifier.fromPath(file);
            if (this.node.getState().hasFile(fileId)) {
                ConsoleLogger.log(SEVERE, "I have the file.");
                this.node.restoreFromOwnStorage(fileId);
                return "Restored from own storage";
            } else {
                ReplicaIdentifier[] replicaIds;
                replicaIds = FileManager.generateReplicaIds(fileId, Node.REPLICATION_DEGREE);
                for (ReplicaIdentifier r: replicaIds) {
                    try {
                        NodeInfo res = this.node.requestRestore(r).get();
                        return "Restored file " + fileId.getFileName() + " from node at " + res.address + ":" + res.port;
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException | NoSuchAlgorithmException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return "Failed to restore file.";
    }

    @Override
    public String delete(String file) throws RemoteException {

        StringBuilder retMsg = new StringBuilder();

        try {
            ReplicaIdentifier[] replicaIds = FileManager.generateReplicaIds(file, Node.REPLICATION_DEGREE);

            ArrayList<CompletableFuture<NodeInfo>> futures = new ArrayList<>();

            for(ReplicaIdentifier replicaId : replicaIds){
                futures.add(this.node.delete(replicaId));
            }

            for(int i = 0; i < futures.size(); i++){
                CompletableFuture<NodeInfo> future = futures.get(i);

                try{
                    NodeInfo result = future.get();
                    retMsg.append("Successfully deleted replica " + i + " with hash " + replicaIds[i].getHash() + " that was in node " + result.id + "\n");
                } catch (InterruptedException | ExecutionException e) {
                    continue;
                }
            }

        } catch (IOException | NoSuchAlgorithmException e) {
            return "Failed to generate replica ids";
        }


        return retMsg.toString();
    }

    @Override
    public String state() throws RemoteException {
        return this.node.getState().toString();
    }

    @Override
    public String reclaim(int newSizeBytes) throws RemoteException {
        StringBuilder retMsg = new StringBuilder();

        ArrayList<ReplicaIdentifier> replicasToDelete = this.node.getState().freeSpace(newSizeBytes);

        ConsoleLogger.log(Level.SEVERE, "Number of replicas to delete: " + Integer.toString(replicasToDelete.size()));

        HashMap<FileIdentifier, byte[]> filesToDelete = new HashMap<>();

        for(ReplicaIdentifier replica : replicasToDelete){
            if(!filesToDelete.containsKey(replica.getFileId())){
                try {
                    filesToDelete.put(replica.getFileId(),
                            FileManager.readFromFile(Paths.get(
                                    Node.NODE_PATH,
                                    "backup",
                                    String.valueOf(replica.getFileId().hashCode())).toString()));
                } catch (ExecutionException | InterruptedException | FileNotFoundException e) {
                    return "Could not find file " + replica.getFileId().fileName + " with hash " + replica.getFileId().hashCode();
                }
            }
        }

        for(FileIdentifier file : filesToDelete.keySet()){
            try {
                FileManager.deleteFile(Paths.get(
                        Node.NODE_PATH,
                        "backup",
                        String.valueOf(file.hashCode())).toString());
            } catch (IOException e) {
                return "Could not delete file " + file.fileName + " with hash " + file.hashCode();
            }
        }

        ArrayList<CompletableFuture<NodeInfo>> futures = new ArrayList<>(replicasToDelete.size());

        for(ReplicaIdentifier replica : replicasToDelete){
            try {
                CompletableFuture<NodeInfo> future = this.node.requestBackup(replica, filesToDelete.get(replica.getFileId()));
                futures.add(future);
            } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        handleBackupFutures(retMsg, futures);

        return retMsg.toString();

    }

    private void handleBackupFutures(StringBuilder retMsg, ArrayList<CompletableFuture<NodeInfo>> futures) {
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


    public synchronized void deleteReplica(DeleteReplicaMessage msg) {
        try {
            if(this.node.getState().deleteReplica(msg.getReplicaId())){

                if(!this.node.getState().hasFileReplicas(msg.getReplicaId().getFileId())) {
                    FileManager.deleteFile(Paths.get(
                            Node.NODE_PATH,
                            "backup",
                            String.valueOf(msg.getReplicaId().getFileId().hashCode())).toString());
                }

                DeleteConfirmationMessage confirmation = new DeleteConfirmationMessage(new SimpleNodeInfo(this.node.getNodeInfo()));

                this.node.getState().removeReplicaLocation(msg.getReplicaId());

                this.node.getCommunicator().send(Utils.createClientSocket(msg.getNode().address, msg.getNode().port), confirmation);

            }else if(this.node.getState().hasReplicaLocation(msg.getReplicaId())){
                SimpleNodeInfo targetNode = this.node.getState().getReplicasLocation().get(msg.getReplicaId());

                DeleteReplicaMessage nextMsg = new DeleteReplicaMessage(msg.getNode(), msg.getReplicaId());

                this.node.getCommunicator().send(Utils.createClientSocket(targetNode.address, targetNode.port), nextMsg);

                this.node.getState().removeReplicaLocation(msg.getReplicaId());

            }
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
