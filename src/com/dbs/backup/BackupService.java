package com.dbs.backup;

import java.rmi.*;

public interface BackupService extends Remote {
    String backup(String file, int replicationDegree) throws RemoteException;
    String restore(String file) throws RemoteException;
    String delete(String file) throws RemoteException;
    String state() throws RemoteException;
}