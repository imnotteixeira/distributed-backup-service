package com.dbs.network.messages;

import com.dbs.backup.FileIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupConfirmMessage extends BackupResponseMessage{
    public BackupConfirmMessage(SimpleNodeInfo node, FileIdentifier fileId) {
        super(MESSAGE_TYPE.BACKUP_CONFIRM, node, fileId);
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

    }
}
