package com.dbs.network.messages;

import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class RestoreRequestMessage extends ChordMessage {

    private final SimpleNodeInfo originNode;
    private final ReplicaIdentifier replicaId;

    public RestoreRequestMessage(SimpleNodeInfo originNode, ReplicaIdentifier replicaId) {
        super(MESSAGE_TYPE.RESTORE_REQUEST);
        this.originNode = originNode;
        this.replicaId = replicaId;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

    }
}
