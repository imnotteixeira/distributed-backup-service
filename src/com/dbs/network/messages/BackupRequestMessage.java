package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupRequestMessage extends ChordMessage{
    private final SimpleNodeInfo originNode;
    private final String fileName;
    private final byte[] data;
    private final BigInteger fileId;

    public BackupRequestMessage(SimpleNodeInfo originNode, BigInteger fileId, String fileName, byte[] data) {
        super(MESSAGE_TYPE.BACKUP_REQUEST);
        this.originNode = originNode;
        this.fileId = fileId;
        this.fileName = fileName;
        this.data = data;

    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handleBackupRequest(this.originNode, this.fileId, this.fileName, this.data);
    }
}
