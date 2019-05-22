package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupRequestMessage extends ChordMessage{
    private final SimpleNodeInfo originNode;
    private final String fileName;
    private final byte[] data;

    public BackupRequestMessage(SimpleNodeInfo originNode, String fileName, byte[] data) {
        super(MESSAGE_TYPE.BACKUP_REQUEST);
        this.originNode = originNode;
        this.fileName = fileName;
        this.data = data;

    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handleBackupRequest(this.originNode, this.fileName, this.data);
    }
}
