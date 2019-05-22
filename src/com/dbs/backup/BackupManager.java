package com.dbs.backup;

import java.rmi.RemoteException;

public class BackupManager implements BackupService {

    public BackupManager() {
    }

    @Override
    public String backup(String file, int replicationDegree, boolean enh) throws RemoteException {
        return null;
    }

    @Override
    public String restore(String file, boolean enh) throws RemoteException {
        return null;
    }

    @Override
    public String delete(String file, boolean enh) throws RemoteException {
        return null;
    }

    @Override
    public void reclaim(int space) throws RemoteException {

    }

    @Override
    public String state() throws RemoteException {
        return null;
    }
}
