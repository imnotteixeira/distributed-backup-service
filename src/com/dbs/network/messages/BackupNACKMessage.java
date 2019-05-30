package com.dbs.network.messages;

import com.dbs.backup.FileIdentifier;
import com.dbs.chord.SimpleNodeInfo;

import java.math.BigInteger;

public class BackupNACKMessage extends BackupResponseMessage {
    public BackupNACKMessage(SimpleNodeInfo node, FileIdentifier fileId) {
        super(MESSAGE_TYPE.BACKUP_NACK, node, fileId);
    }
}
