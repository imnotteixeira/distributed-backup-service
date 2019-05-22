package com.dbs.backup;

import com.dbs.utils.ConsoleLogger;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import static java.util.logging.Level.*;

public class BackupManager implements BackupService {

    private String accessPoint;

    public BackupManager(String accessPoint) {
        this.accessPoint = accessPoint;

        BackupService service = null;
        try {
            service = (BackupService) UnicastRemoteObject.exportObject(this, 0);
            LocateRegistry.getRegistry().rebind(this.accessPoint, service);
            ConsoleLogger.log(WARNING, "Found existing RMI registry");
        } catch (Exception e) {
            try {
                LocateRegistry.createRegistry(1099).rebind(this.accessPoint, service);
                ConsoleLogger.log(WARNING, "Created new RMI registry");
            } catch (Exception exc) {
                exc.printStackTrace();
            }
        }
    }

    @Override
    public String backup(String file, int replicationDegree) throws RemoteException {
        ConsoleLogger.log(INFO,"Starting backup");
        return null;
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
