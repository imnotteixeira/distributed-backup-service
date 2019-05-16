package com.dbs.chord.operations;

import com.dbs.network.messages.ChordMessage;

public class OperationEntry {
    ChordMessage.MESSAGE_TYPE type;

    public OperationEntry(ChordMessage.MESSAGE_TYPE type) {
        this.type = type;
    }
}
