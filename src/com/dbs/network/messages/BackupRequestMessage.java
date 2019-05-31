package com.dbs.network.messages;

import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.utils.ConsoleLogger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class BackupRequestMessage extends ChordMessage{
    private final SimpleNodeInfo responseSocketInfo;
    private final SimpleNodeInfo originNode;

    private final ReplicaIdentifier replicaId;

    private final boolean isOriginalRequest;

    public BackupRequestMessage(SimpleNodeInfo responseSocketInfo, SimpleNodeInfo originNode, ReplicaIdentifier replicaId, boolean isOriginalRequest) {
        super(MESSAGE_TYPE.BACKUP_REQUEST);
        this.responseSocketInfo = responseSocketInfo;
        this.originNode = originNode;
        this.isOriginalRequest = isOriginalRequest;

        this.replicaId = replicaId;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        ConsoleLogger.log(Level.SEVERE, "RECEIVED A BACKUP REQUEST");
        n.handleBackupRequest(this);
    }

    public ReplicaIdentifier getReplicaId() {
        return replicaId;
    }

    public SimpleNodeInfo getResponseSocketInfo() {
        return this.responseSocketInfo;
    }

    public SimpleNodeInfo getOriginNode() {
        return originNode;
    }

    public boolean isOriginalRequest() {
        return isOriginalRequest;
    }
}
