package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;


/**
 * Sent when a node wants to find a successor
 */
public class FindSuccessorMessage extends ChordMessage {

    private SimpleNodeInfo originNode;
    private BigInteger key;

    public FindSuccessorMessage(SimpleNodeInfo originNode, BigInteger key) {
        super(MESSAGE_TYPE.FIND_SUCCESSOR);
        this.originNode = originNode;
        this.key = key;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handleSuccessorRequest(this.originNode, this.key);
    }
}
