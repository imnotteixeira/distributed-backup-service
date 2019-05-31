package com.dbs.protocols.reclaim;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.filemanager.FileManager;
import com.dbs.protocols.backup.FileIdentifier;
import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.utils.ConsoleLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;


public class ReclaimManager {

    private final Node node;

    public ReclaimManager(Node node) {
        this.node = node;

    }

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
                    return "Could not find file " + replica.getFileId().getFileName() + " with hash " + replica.getFileId().hashCode();
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
                return "Could not delete file " + file.getFileName() + " with hash " + file.hashCode();
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

        this.node.getBackupManager().handleBackupFutures(retMsg, futures);

        return retMsg.toString();

    }
}
