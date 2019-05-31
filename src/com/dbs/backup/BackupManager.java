package com.dbs.backup;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.chord.Utils;
import com.dbs.filemanager.FileManager;
import com.dbs.network.messages.BackupACKMessage;
import com.dbs.network.messages.BackupConfirmMessage;
import com.dbs.network.messages.BackupRequestMessage;
import com.dbs.network.messages.BackupResponseMessage;
import com.dbs.utils.ConsoleLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();

        StringBuilder retMsg = new StringBuilder();

        for (CompletableFuture<NodeInfo> future : futures) {

            if(future.isDone()) {
                BigInteger id = null;
                try {
                    id = future.get().id;
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                if(future.isCompletedExceptionally()) {
                    retMsg.append("Tried to save replica in node " + id + " but could not.\n");
                } else {
                    retMsg.append("Successfully saved replica in node " + id + ".\n");
                }
            }

        }


        return "File Backed Up in "+ futures.size() + " nodes!\n" + retMsg.toString();
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

    public void storeReplica(BackupRequestMessage request) throws IOException, NoSuchAlgorithmException {

        try {
            BackupResponseMessage msg;

            if(this.node.getState().addReplica(request.getReplicaId())){
                System.out.println("I already have the file!");
                msg = new BackupConfirmMessage(new SimpleNodeInfo(this.node.getNodeInfo()), request.getReplicaId());
            }else{
                System.out.println("I Dont have the file yet, but you can send!");
                msg = new BackupACKMessage(new SimpleNodeInfo(this.node.getNodeInfo()), request.getReplicaId());
            }

            System.out.println("Sending the above response to " + request.getOriginNode().address + ":" + request.getOriginNode().port);
            this.node.getCommunicator().send(Utils.createClientSocket(request.getOriginNode().address, request.getOriginNode().port), msg);
        } catch (NoSpaceException e) {

            BackupRequestMessage msg = new BackupRequestMessage(request.getOriginNode(), request.getReplicaId());

            this.node.getCommunicator().send(Utils.createClientSocket(this.node.getSuccessor().address, this.node.getSuccessor().port), msg);
        }
    }


    @Override
    public String restore(String file) throws RemoteException {
        ConsoleLogger.log(INFO,"Starting restore");

        try {
            FileIdentifier fileId = FileIdentifier.fromPath(file);
            if (this.node.getState().hasFile(fileId)) {
                ConsoleLogger.log(SEVERE, "I have the file.");
                // get the file
            } else {
                ReplicaIdentifier[] replicaIds;
                replicaIds = FileManager.generateReplicaIds(fileId, Node.REPLICATION_DEGREE);
                for (ReplicaIdentifier r: replicaIds) {
                    NodeInfo res = this.node.requestRestore(r).get();
                    ConsoleLogger.log(SEVERE, "Restored file " + fileId.getFileName() + " from node " + res.getAccessPoint());
                    break;
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
