package com.dbs.network.messages;

import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupConfirmMessage extends BackupResponseMessage{
    public BackupConfirmMessage(SimpleNodeInfo node, ReplicaIdentifier replicaId) {
        super(MESSAGE_TYPE.BACKUP_CONFIRM, node, replicaId);
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

    }
}
