package com.dbs.network.messages;

import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.chord.SimpleNodeInfo;

public class BackupNACKMessage extends BackupResponseMessage {
    public BackupNACKMessage(SimpleNodeInfo node, ReplicaIdentifier fileId) {
        super(MESSAGE_TYPE.BACKUP_NACK, node, fileId);
    }
}
