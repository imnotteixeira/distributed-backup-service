package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class StatusCheckConfirmMessage extends NodeInfoMessage {

    public StatusCheckConfirmMessage(SimpleNodeInfo node) {
        super(MESSAGE_TYPE.STATUS_CHECK_CONFIRM, node);
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

    }
}
