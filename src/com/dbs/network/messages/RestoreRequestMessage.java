package com.dbs.network.messages;

import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class RestoreRequestMessage extends ChordMessage {

    private final SimpleNodeInfo requestSocketInfo;
    private final SimpleNodeInfo originNode;
    private final ReplicaIdentifier replicaId;

    public RestoreRequestMessage(SimpleNodeInfo responseSocketInfo, SimpleNodeInfo originNode, ReplicaIdentifier replicaId) {
        super(MESSAGE_TYPE.RESTORE_REQUEST);
        this.originNode = originNode;
        this.replicaId = replicaId;
        this.requestSocketInfo = responseSocketInfo;
    }

    @Override
    public void handle(Node n) throws IOException, ExecutionException, InterruptedException, NoSuchAlgorithmException {
        n.handleRestoreRequest(this);
    }

    public ReplicaIdentifier getReplicaId() {
        return replicaId;
    }

    public SimpleNodeInfo getOriginNode() {
        return originNode;
    }

    public SimpleNodeInfo getRequestSocketInfo() {
        return requestSocketInfo;
    }
}
