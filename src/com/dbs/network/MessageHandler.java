package com.dbs.network;

import com.dbs.chord.Node;
import com.dbs.network.messages.ChordMessage;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public class MessageHandler {

    public static void handle(Object obj, Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        ChordMessage msg = ChordMessage.fromObject(obj);
        msg.handle(n);
    }
}
