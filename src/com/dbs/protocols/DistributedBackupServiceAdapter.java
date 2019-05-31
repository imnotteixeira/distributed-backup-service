package com.dbs.protocols;

import com.dbs.chord.Node;
import com.dbs.utils.ConsoleLogger;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import static java.util.logging.Level.WARNING;

public class DistributedBackupServiceAdapter implements IDistributedBackupService {

    private final Node node;

    public DistributedBackupServiceAdapter(Node node) {
        this.node = node;
        IDistributedBackupService service = null;
        try {
            service = (IDistributedBackupService) UnicastRemoteObject.exportObject(this, 0);
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
    public String backup(String file, int repDegree) throws RemoteException {
        file = "testfiles/" + file;
        return node.getBackupManager().backup(file, repDegree);
    }

    @Override
    public String state() throws RemoteException {
        return node.getBackupManager().state();
    }

    @Override
    public String restore(String file) throws RemoteException {
        file = "testfiles/" + file;
        return node.getRestoreManager().restore(file);
    }

    @Override
    public String delete(String file) throws RemoteException {
        file = "testfiles/" + file;
        return node.getDeleteManager().delete(file);
    }

    @Override
    public String reclaim(int newSizeBytes) throws RemoteException {
        return node.getReclaimManager().reclaim(newSizeBytes);
    }
}
