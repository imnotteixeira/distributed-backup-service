package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.chord.operations.PredecessorRequestOperationEntry;
import com.dbs.network.NullNodeInfo;
import com.dbs.network.NullSimpleNodeInfo;
import com.dbs.utils.ConsoleLogger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class PredecessorMessage extends NodeInfoMessage {

    SimpleNodeInfo sender;

    public PredecessorMessage(SimpleNodeInfo sender, SimpleNodeInfo predecessor) {
        super(MESSAGE_TYPE.PREDECESSOR, predecessor);
        this.sender = sender;
    }

    public PredecessorMessage(SimpleNodeInfo sender, NullNodeInfo predecessor) throws IOException, NoSuchAlgorithmException {
        super(MESSAGE_TYPE.PREDECESSOR, new NullSimpleNodeInfo());
        this.sender = sender;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        n.concludeOperation(new PredecessorRequestOperationEntry(sender));
    }

}
