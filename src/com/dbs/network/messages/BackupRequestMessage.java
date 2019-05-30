package com.dbs.network.messages;

import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupRequestMessage extends ChordMessage{
    private final SimpleNodeInfo originNode;
//    private final byte[] data;
    private final ReplicaIdentifier replicaId;

    public BackupRequestMessage(SimpleNodeInfo originNode, ReplicaIdentifier replicaId) {
        super(MESSAGE_TYPE.BACKUP_REQUEST);
        this.originNode = originNode;
        this.replicaId = replicaId;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handleBackupRequest(this);
    }

    public ReplicaIdentifier getReplicaId() {
        return replicaId;
    }

    public SimpleNodeInfo getOriginNode() {
        return this.originNode;
    }
}
