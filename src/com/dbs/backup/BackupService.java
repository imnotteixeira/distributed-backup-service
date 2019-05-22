package com.dbs.backup;

import java.rmi.*;

public interface BackupService extends Remote {
    String backup(String file, int replicationDegree, boolean enh) throws RemoteException;
    String restore(String file, boolean enh) throws RemoteException;
    String delete(String file, boolean enh) throws RemoteException;
    void reclaim(int space) throws RemoteException;
    String state() throws RemoteException;
}