package com.dbs.network.messages;

import com.dbs.chord.SimpleNodeInfo;

import java.math.BigInteger;

public class BackupACKMessage extends BackupResponseMessage {
    public BackupACKMessage(SimpleNodeInfo node, BigInteger fileId) {
        super(MESSAGE_TYPE.BACKUP_ACK, node, fileId);
    }
}
