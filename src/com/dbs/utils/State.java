package com.dbs.utils;

import com.dbs.protocols.backup.FileIdentifier;
import com.dbs.protocols.backup.NoSpaceException;
import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;

import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class  State implements Serializable {

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


    public SimpleNodeInfo getReplicaLocation(ReplicaIdentifier id) {
        return this.replicasLocation.getOrDefault(id, null);
    }

    public boolean deleteReplica(ReplicaIdentifier id) {

        if (hasReplica(id)) {
            localReplicas.get(id.getFileId()).remove(id);
            if(localReplicas.get(id.getFileId()).isEmpty()){
                localReplicas.remove(id.getFileId());
            }
            return true;
        }
        return false;
    }

    public void deleteFile(FileIdentifier id) {
        if (hasFile(id)) {
            localReplicas.remove(id);
        }
    }

    private String storedFilesString() {
        StringBuilder files = new StringBuilder();

        if (localReplicas.isEmpty()) {
            return "No stored files.\n";
        }

        for (Map.Entry<FileIdentifier, HashSet<ReplicaIdentifier>> entry : localReplicas.entrySet()) {
            files.append("File: ").append(entry.getKey().getFileName()).append("\n");
            files.append("Size: ").append(entry.getKey().getFileSize()).append("\n");
            files.append("Number of Replicas: ").append(entry.getValue().size()).append("\n\n");
        }

        return files.toString();
    }

    private String replicaLocationsString() {
        StringBuilder result = new StringBuilder();

        if (replicasLocation.isEmpty()) {
            return "No known replica locations.\n";
        }

        replicasLocation.forEach((key, value) -> {
            try {
                result.append("Replica number " + key.getHash() + " of file " + key.getFileId().getFileName() + " is in node " + new NodeInfo(value.address, value.port).id + "\n");
            } catch (NoSuchAlgorithmException e) {}
        });

        return result.toString();
    }

    @Override
    public String toString() {
        String state;
        state = "[STATE]\n";
        state += "\nBacked up files:\n";
        state += storedFilesString();
        state += "\nReplica locations:\n";
        state += replicaLocationsString();
        state += "\nUsed space: " +  this.getSpace();
        state += "\nMax space: " + this.maxSpace + "\n";
        return state;
    }


    public ConcurrentHashMap<ReplicaIdentifier, SimpleNodeInfo> getReplicasLocation() {
        return replicasLocation;
    }

    public ArrayList<ReplicaIdentifier> freeSpace(int newSizeBytes) {

        int minimumSpaceToFree = this.maxSpace - newSizeBytes;

        setMaxSpace(newSizeBytes);

        if(minimumSpaceToFree <= 0){
            return new ArrayList<>();
        }

        ArrayList<ReplicaIdentifier> replicasToDelete = new ArrayList<>();
        int totalFreedSpace = 0;

        for(FileIdentifier file : localReplicas.keySet()){
            if(totalFreedSpace >= minimumSpaceToFree){
                break;
            }

            for(ReplicaIdentifier replica : this.localReplicas.get(file)){
                replicasToDelete.add(replica);
            }

            totalFreedSpace += file.getFileSize();
            deleteFile(file);
        }

        return replicasToDelete;
    }

    public boolean hasReplicaLocation(ReplicaIdentifier replicaId) {
        return this.replicasLocation.containsKey(replicaId);
    }

    public void removeReplicaLocation(ReplicaIdentifier replicaId) {
        this.replicasLocation.remove(replicaId);
    }

    public boolean hasFileReplicas(FileIdentifier id) {
        return this.localReplicas.containsKey(id);
    }
}
