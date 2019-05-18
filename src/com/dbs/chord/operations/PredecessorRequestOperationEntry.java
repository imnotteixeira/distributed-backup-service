package com.dbs.chord.operations;

import com.dbs.chord.SimpleNodeInfo;
import com.dbs.network.messages.ChordMessage;

public class PredecessorRequestOperationEntry extends OperationEntry {


    private final SimpleNodeInfo successor;

    public PredecessorRequestOperationEntry(SimpleNodeInfo successor) {

        super(ChordMessage.MESSAGE_TYPE.PREDECESSOR);
        this.successor = successor;
    }
}
