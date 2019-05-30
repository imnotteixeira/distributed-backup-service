package com.dbs.network.messages;

import com.dbs.backup.FileIdentifier;
import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class BackupRequestMessage extends ChordMessage{
    private final SimpleNodeInfo originNode;
//    private final byte[] data;
    private final FileIdentifier fileId;
    private final long fileSize;

    public BackupRequestMessage(SimpleNodeInfo originNode, FileIdentifier fileId, long fileSize) {
        super(MESSAGE_TYPE.BACKUP_REQUEST);
        this.originNode = originNode;
        this.fileId = fileId;
        this.fileSize = fileSize;
//        this.data = data;

    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handleBackupRequest(this);
    }

    public FileIdentifier getFileId() {
        return fileId;
    }

    public long getFileSize() {
        return fileSize;
    }

    public SimpleNodeInfo getOriginNode() {
        return this.originNode;
    }
}
