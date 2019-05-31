package com.dbs.network.messages;

import com.dbs.chord.Node;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class NotFoundMessage extends ChordMessage {
    public NotFoundMessage() {
        super(MESSAGE_TYPE.NOT_FOUND);
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {

    }
}
