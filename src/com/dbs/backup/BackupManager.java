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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

    public boolean hasReplica(ReplicaIdentifier id) {
        return this.node.getState().hasReplica(id);
    }

    public boolean hasFile(FileIdentifier id) {
        return this.node.getState().hasFile(id);
    }

    public synchronized boolean canStore(long fileSize) {
        return this.node.getState().hasSpace(fileSize);
    }

    public void checkStoreReplica(BackupRequestMessage request) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

        try {
            BackupResponseMessage msg;

            if(this.node.getState().hasFileToStore(request.getReplicaId())){
                System.out.println("I already have the file!");
                msg = new BackupConfirmMessage(new SimpleNodeInfo(this.node.getNodeInfo()), request.getReplicaId());
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
            } else {
                ReplicaIdentifier[] replicaIds;
                replicaIds = FileManager.generateReplicaIds(fileId, Node.REPLICATION_DEGREE);
                for (ReplicaIdentifier r: replicaIds) {
                    try {
                        NodeInfo res = this.node.requestRestore(r).get();
                        ConsoleLogger.log(SEVERE, "Restored file " + fileId.getFileName() + " from node " + res.getAccessPoint());
                        break;
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException | NoSuchAlgorithmException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public String delete(String file) throws RemoteException {
        ConsoleLogger.log(INFO,"Starting delete");
        return null;
    }

    @Override
    public String state() throws RemoteException {
        ConsoleLogger.log(INFO,"Printing state");
        return null;
    }



}
