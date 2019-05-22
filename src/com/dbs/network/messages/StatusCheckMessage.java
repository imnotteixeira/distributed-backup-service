package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class StatusCheckMessage extends NodeInfoMessage {
    SimpleNodeInfo originNode;

    public StatusCheckMessage(SimpleNodeInfo originNode) {
        super(MESSAGE_TYPE.STATUS_CHECK, originNode);
        this.originNode = originNode;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handleStatusCheck(this.originNode);
    }
}
