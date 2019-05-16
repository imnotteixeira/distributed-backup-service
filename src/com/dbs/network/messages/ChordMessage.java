package com.dbs.network.messages;

import com.dbs.chord.Node;

import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;


public abstract class ChordMessage implements Serializable {
    public static ChordMessage fromObject(Object obj) throws ClassCastException {
        if(obj instanceof ChordMessage) {
            ChordMessage msg = (ChordMessage) obj;

            switch (msg.type) {
                case FIND_SUCCESSOR:
                    return (FindSuccessorMessage) obj;
                default:
                    return msg;
            }
        } else throw new ClassCastException();
    }

    public enum MESSAGE_TYPE {
        FIND_SUCCESSOR,
        SUCCESSOR
    }

    private MESSAGE_TYPE type;

    public ChordMessage(MESSAGE_TYPE type) {
        this.type = type;
    }

    public abstract void handle(Node n) throws IOException, NoSuchAlgorithmException;
}
