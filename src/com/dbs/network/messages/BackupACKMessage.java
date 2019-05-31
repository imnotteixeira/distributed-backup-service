package com.dbs.network.messages;

import com.dbs.protocols.backup.ReplicaIdentifier;
import com.dbs.chord.SimpleNodeInfo;

public class BackupACKMessage extends BackupResponseMessage {
    public BackupACKMessage(SimpleNodeInfo node, ReplicaIdentifier fileId) {
        super(MESSAGE_TYPE.BACKUP_ACK, node, fileId);
    }
}
