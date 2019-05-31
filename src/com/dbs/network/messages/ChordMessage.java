package com.dbs.network.messages;

import com.dbs.chord.Node;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;


public abstract class ChordMessage implements Serializable {
    public static ChordMessage fromObject(Object obj) throws ClassCastException {
        if(obj instanceof ChordMessage) {
            ChordMessage msg = (ChordMessage) obj;

            switch (msg.type) {
                case FIND_SUCCESSOR:
                    return (FindSuccessorMessage) obj;
                case FETCH_PREDECESSOR:
                    return (FetchPredecessorMessage) obj;
                case NOTIFY_SUCCESSOR:
                    return (NotifySuccessorMessage) obj;
                case SUCCESSOR:
                    return (SuccessorMessage) obj;
                case PREDECESSOR:
                    return (PredecessorMessage) obj;
                case STATUS_CHECK:
                    return (StatusCheckMessage) obj;
                case STATUS_CHECK_CONFIRM:
                    return (StatusCheckConfirmMessage) obj;
                case BACKUP_REQUEST:
                    return (BackupRequestMessage) obj;
                case BACKUP_CONFIRM:
                    return (BackupConfirmMessage) obj;
                case BACKUP_ACK:
                    return (BackupACKMessage) obj;
                case BACKUP_NACK:
                    return (BackupNACKMessage) obj;
                case BACKUP_PAYLOAD:
                    return (BackupPayloadMessage) obj;
                case RESTORE_REQUEST:
                    return (RestoreRequestMessage) obj;
                case RESTORE_PAYLOAD:
                    return (RestorePayloadMessage) obj;
                case DELETE:
                    return (DeleteReplicaMessage) obj;
                case DELETE_CONFIRM:
                    return (DeleteConfirmationMessage) obj;
                case NOT_FOUND:
                    return (NotFoundMessage) obj;
                case UPDATE_REPLICA_LOCATION:
                    return (UpdateReplicaLocationMessage) obj;
                default:
                    return msg;
            }
        } else throw new ClassCastException();
    }

    public enum MESSAGE_TYPE {
        FETCH_PREDECESSOR,
        FIND_SUCCESSOR,
        SUCCESSOR,
        PREDECESSOR,
        NOTIFY_SUCCESSOR,
        STATUS_CHECK,
        STATUS_CHECK_CONFIRM,
        BACKUP_REQUEST,
        BACKUP_CONFIRM,
        BACKUP_ACK,
        BACKUP_NACK,
        BACKUP_PAYLOAD,
        RESTORE_REQUEST,
        RESTORE_PAYLOAD,
        DELETE,
        DELETE_CONFIRM,
        NOT_FOUND,
        UPDATE_REPLICA_LOCATION
    }

    private MESSAGE_TYPE type;

    public ChordMessage(MESSAGE_TYPE type) {
        this.type = type;
    }

    public abstract void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException;
}
