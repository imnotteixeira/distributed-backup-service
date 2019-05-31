package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class DeleteConfirmationMessage extends NodeInfoMessage{

    public DeleteConfirmationMessage(SimpleNodeInfo node) {
        super(MESSAGE_TYPE.DELETE_CONFIRM, node);
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

    }
}
