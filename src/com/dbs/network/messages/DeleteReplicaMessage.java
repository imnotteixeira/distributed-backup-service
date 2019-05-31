package com.dbs.network.messages;

import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class DeleteReplicaMessage extends NodeInfoMessage{

    ReplicaIdentifier replicaId;

    public DeleteReplicaMessage(SimpleNodeInfo responseSocketInfo, ReplicaIdentifier replicaId) {
        super(MESSAGE_TYPE.DELETE, responseSocketInfo);
        this.replicaId = replicaId;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handleReplicaDeletion(this);
    }

    public ReplicaIdentifier getReplicaId() {
        return replicaId;
    }
}
