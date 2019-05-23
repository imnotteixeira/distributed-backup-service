package com.dbs.backup;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.filemanager.FileManager;
import com.dbs.utils.ConsoleLogger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

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

        BigInteger[] fileIds;
        byte[] fileContent;
        try {
            fileIds = FileManager.generateFileIds(file, Node.REPLICATION_DEGREE);
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RemoteException("Could not generate file ids", e);
        }

        try {
            fileContent = FileManager.readFromFile(file);
        } catch (ExecutionException | InterruptedException | FileNotFoundException e) {
            throw new RemoteException("Could not read file contents", e);
        }

        ArrayList<CompletableFuture<NodeInfo>> futures = initBackupOperation(fileIds, FileManager.getFileName(file), fileContent);

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

    private ArrayList<CompletableFuture<NodeInfo>> initBackupOperation(BigInteger[] fileIds, String fileName, byte[] fileContent) {

        ArrayList<CompletableFuture<NodeInfo>> futures = new ArrayList<>(fileIds.length);

        for (int i = 0; i < fileIds.length; i++) {
            try {
                CompletableFuture<NodeInfo> currRequest = this.node.requestBackup(fileIds[i], fileName, fileContent);

                futures.add(i,currRequest);
            } catch (IOException | NoSuchAlgorithmException | ExecutionException | InterruptedException e) {
                //continue
            }
        }

        return futures;

    }
    
    

    @Override
    public String restore(String file) throws RemoteException {
        ConsoleLogger.log(INFO,"Starting restore");
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
