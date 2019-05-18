package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class NotifySuccessorMessage extends ChordMessage {

    SimpleNodeInfo originNode;

    public NotifySuccessorMessage(SimpleNodeInfo originNode) {
        super(MESSAGE_TYPE.NOTIFY_SUCCESSOR);
        this.originNode = originNode;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handlePredecessorNotification(this.originNode);
    }
}
