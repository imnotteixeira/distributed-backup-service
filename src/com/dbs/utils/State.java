package com.dbs.utils;

import com.dbs.backup.FileIdentifier;
import com.dbs.backup.NoSpaceException;
import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class State implements Serializable {

    private int maxSpace;

    private ConcurrentHashMap<FileIdentifier, HashSet<ReplicaIdentifier>> localReplicas;
    private ConcurrentHashMap<ReplicaIdentifier, SimpleNodeInfo> replicasLocation;

    public State() {

        this.maxSpace = Node.INITIAL_SPACE_LIMIT_BYTES;
        this.localReplicas = new ConcurrentHashMap<>();
        this.replicasLocation = new ConcurrentHashMap<>();
    }

    public void setMaxSpace(int maxSpace) {
        this.maxSpace = maxSpace;
    }

    private int getSpace() {
        int space = 0;
        for (FileIdentifier fileId : localReplicas.keySet()) {
            space += fileId.getFileSize();
        }

        return space;
    }

    public boolean hasSpace(long fileSize) {
        return this.maxSpace > (fileSize + this.getSpace());
    }

    /**
     *
     * @param replicaId
     * @return true if the file was already stored. false if it was not.
     * @throws NoSpaceException - the replica was not stored
     */
    public synchronized boolean hasFileToStore(ReplicaIdentifier replicaId) throws NoSpaceException {
        if(hasReplica(replicaId)) return true;

        if(hasFile(replicaId.getFileId())){
            return true;
        }

        if (hasSpace(replicaId.getFileId().getFileSize())){
            return false;
        }
        throw new NoSpaceException();
    }

    /**
     *
     * @param replicaId
     * @return true if the file was already stored. false if it was not. Either way, the replica is stored
     * @throws NoSpaceException - the replica was not stored
     */
    public synchronized boolean addReplica(ReplicaIdentifier replicaId) throws NoSpaceException {
        if(hasReplica(replicaId)) return true;

        if(hasFile(replicaId.getFileId())){
            localReplicas.get(replicaId.getFileId()).add(replicaId);
            return true;
        }

        if (hasSpace(replicaId.getFileId().getFileSize())){
            HashSet<ReplicaIdentifier> newSet = new HashSet<>();
            newSet.add(replicaId);
            localReplicas.put(replicaId.getFileId(), newSet);
            return false;
        }
        throw new NoSpaceException();
    }

    public synchronized void setReplicaLocation(ReplicaIdentifier replicaId, SimpleNodeInfo location) {
        replicasLocation.put(replicaId, location);
    }

    public boolean hasFile(FileIdentifier id) {
        return this.localReplicas.keySet().contains(id);
    }

    public boolean hasReplica(ReplicaIdentifier id) {
        return hasFile(id.getFileId()) && this.localReplicas.get(id.getFileId()).contains(id);
    }

    public void deleteReplica(ReplicaIdentifier id) {
        if (hasReplica(id)) {
            localReplicas.get(id.getFileId()).remove(id);
            //TODO É PRECISO APAGAR O FICHEIRO SE FOR A ULTIMA REPLICA
        }
    }

    public void deleteFile(FileIdentifier id) {
        if (hasFile(id)) {
            localReplicas.remove(id);
        }
    }

    private String storedFilesString() {
        StringBuilder files = new StringBuilder();

        if (localReplicas.isEmpty()) {
            return "No stored chunks.\n";
        }

        for (Map.Entry<FileIdentifier, HashSet<ReplicaIdentifier>> entry : localReplicas.entrySet()) {
            files.append("File: ").append(entry.getKey().getFileName()).append("\n");
            files.append("Size: ").append(entry.getKey().getFileSize()).append("\n");
            files.append("Number of Replicas: ").append(entry.getValue().size()).append("\n");
        }

        return files.toString();
    }

    @Override
    public String toString() {
        String state;
        state = "[STATE]\n";
        state += "\nBacked up files:\n";
        state += storedFilesString();
        state += "\nUsed space: " +  this.getSpace();
        state += "\nMax space: " + this.maxSpace + "\n";
        return state;
    }

    public ConcurrentHashMap<ReplicaIdentifier, SimpleNodeInfo> getReplicasLocation() {
        return replicasLocation;
    }
}
