package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;


/**
 * Sent as an answer to find successor request
 */
public class SuccessorMessage extends ChordMessage{

    SimpleNodeInfo successor;

    public SuccessorMessage(SimpleNodeInfo successor) {
        super(MESSAGE_TYPE.SUCCESSOR);
        this.successor = successor;
    }

    @Override
    public void handle(Node n) {

    }

    public SimpleNodeInfo getSuccessor() {
        return successor;
    }
}
