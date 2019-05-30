package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupResponseMessage extends NodeInfoMessage {
    private final BigInteger fileId;

    public BackupResponseMessage(MESSAGE_TYPE type, SimpleNodeInfo node, BigInteger fileId) {
        super(type, node);
        this.fileId = fileId;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

    }
}
