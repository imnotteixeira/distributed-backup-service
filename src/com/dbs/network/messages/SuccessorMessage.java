package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.chord.operations.SuccessorRequestOperationEntry;

import java.math.BigInteger;


/**
 * Sent as an answer to find successor request
 */
public class SuccessorMessage extends NodeInfoMessage {

    BigInteger key;

    public SuccessorMessage(BigInteger key, SimpleNodeInfo successor) {
        super(MESSAGE_TYPE.SUCCESSOR, successor);
        this.key = key;
    }

    @Override
    public void handle(Node n) {
        n.concludeOperation(new SuccessorRequestOperationEntry(key));
    }

}
