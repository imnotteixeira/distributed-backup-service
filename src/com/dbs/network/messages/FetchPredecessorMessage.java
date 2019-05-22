package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.utils.ConsoleLogger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class FetchPredecessorMessage extends ChordMessage {

    private SimpleNodeInfo originNode;

    public FetchPredecessorMessage(SimpleNodeInfo originNode) {
        super(MESSAGE_TYPE.FETCH_PREDECESSOR);
        this.originNode = originNode;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        ConsoleLogger.log(Level.WARNING, "Received FETCH PREDECESSOR Message");
        n.handlePredecessorRequest(this.originNode);
    }
}