package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class StatusCheckMessage extends NodeInfoMessage {
    SimpleNodeInfo responseSocketInfo;

    public StatusCheckMessage(SimpleNodeInfo responseSocketInfo) {
        super(MESSAGE_TYPE.STATUS_CHECK, responseSocketInfo);
        this.responseSocketInfo = responseSocketInfo;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.handleStatusCheck(this.responseSocketInfo);
    }
}
