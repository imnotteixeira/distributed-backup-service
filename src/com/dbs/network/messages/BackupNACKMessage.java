package com.dbs.network.messages;

import com.dbs.chord.SimpleNodeInfo;

import java.math.BigInteger;

public class BackupNACKMessage extends BackupResponseMessage {
    public BackupNACKMessage(SimpleNodeInfo node, BigInteger fileId) {
        super(MESSAGE_TYPE.BACKUP_NACK, node, fileId);
    }
}
