package com.dbs.chord.operations;

import com.dbs.network.messages.ChordMessage;

import java.math.BigInteger;

public class SuccessorRequestOperationEntry extends OperationEntry {

    private BigInteger key;

    public SuccessorRequestOperationEntry(BigInteger key) {

        super(ChordMessage.MESSAGE_TYPE.FIND_SUCCESSOR);
        this.key = key;
    }
}
