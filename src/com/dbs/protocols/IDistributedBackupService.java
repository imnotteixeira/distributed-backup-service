package com.dbs.protocols;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDistributedBackupService extends Remote {

    String backup(String file, int repDegree) throws RemoteException;
    String state() throws RemoteException;
    String restore(String file) throws RemoteException;
    String delete(String file) throws RemoteException;
    String reclaim(int newSizeBytes) throws RemoteException;

}

