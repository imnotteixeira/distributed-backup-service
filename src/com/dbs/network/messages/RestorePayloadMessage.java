package com.dbs.network.messages;

import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.utils.ConsoleLogger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class RestorePayloadMessage extends ChordMessage {

    SimpleNodeInfo originNode;
    ReplicaIdentifier replicaId;
    byte[] data;

    public RestorePayloadMessage(SimpleNodeInfo originNode, ReplicaIdentifier replicaId, byte[] data) {
        super(MESSAGE_TYPE.RESTORE_PAYLOAD);
        this.originNode = originNode;
        this.replicaId = replicaId;
        this.data = data;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        ConsoleLogger.log(Level.SEVERE, "RECEIVED PAYLOAD!");
    }

    public SimpleNodeInfo getOriginNode() {
        return originNode;
    }

    public ReplicaIdentifier getReplicaId() {
        return replicaId;
    }

    public byte[] getData() {
        return data;
    }
}
