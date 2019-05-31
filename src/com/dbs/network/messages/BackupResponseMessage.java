package com.dbs.network.messages;

import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupResponseMessage extends NodeInfoMessage {
    private final ReplicaIdentifier fileId;

    public BackupResponseMessage(MESSAGE_TYPE type, SimpleNodeInfo node, ReplicaIdentifier fileId) {
        super(type, node);
        this.fileId = fileId;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

    }
}
