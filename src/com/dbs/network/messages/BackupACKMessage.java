package com.dbs.network.messages;

import com.dbs.backup.FileIdentifier;
import com.dbs.chord.SimpleNodeInfo;

import java.math.BigInteger;

public class BackupACKMessage extends BackupResponseMessage {
    public BackupACKMessage(SimpleNodeInfo node, FileIdentifier fileId) {
        super(MESSAGE_TYPE.BACKUP_ACK, node, fileId);
    }
}
