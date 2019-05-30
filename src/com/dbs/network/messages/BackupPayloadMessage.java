package com.dbs.network.messages;

import com.dbs.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupPayloadMessage extends ChordMessage {

    SimpleNodeInfo originNode;
    ReplicaIdentifier replicaId;
    byte[] data;

    public BackupPayloadMessage(SimpleNodeInfo originNode, ReplicaIdentifier replicaId, byte[] data) {
        super(MESSAGE_TYPE.BACKUP_PAYLOAD);
        this.originNode = originNode;
        this.replicaId = replicaId;
        this.data = data;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        //TODO node.store
    }
}
