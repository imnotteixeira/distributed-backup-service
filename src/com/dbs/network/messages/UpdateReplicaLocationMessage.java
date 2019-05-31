package com.dbs.network.messages;

import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class UpdateReplicaLocationMessage extends NodeInfoMessage{
    private final ReplicaIdentifier replicaId;

    public UpdateReplicaLocationMessage(ReplicaIdentifier replicaId, SimpleNodeInfo location) {
        super(MESSAGE_TYPE.UPDATE_REPLICA_LOCATION, location);
        this.replicaId = replicaId;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.updateReplicaLocation(replicaId, node);
    }

    public ReplicaIdentifier getReplicaId() {
        return replicaId;
    }
}
