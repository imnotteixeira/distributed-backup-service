package com.dbs.network.messages;

import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;
import com.dbs.utils.ConsoleLogger;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;


/**
 * Sent when a node wants to find a successor
 */
public class FindSuccessorMessage extends ChordMessage {

    private SimpleNodeInfo responseSocketInfo;
    private BigInteger key;

    public FindSuccessorMessage(SimpleNodeInfo responseSocketInfo, BigInteger key) {
        super(MESSAGE_TYPE.FIND_SUCCESSOR);
        this.responseSocketInfo = responseSocketInfo;
        this.key = key;
    }

    @Override
    public void handle(Node n) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException {
        ConsoleLogger.log(Level.WARNING, "Received FETCH SUCCESSOR Message");

        n.handleSuccessorRequest(this.responseSocketInfo, this.key);
    }

    @Override
    public String toString() {
        return "FindSuccessorMessage{" +
                "responseSocketInfo=" + responseSocketInfo.toString() +
                ", key=" + key +
                '}';
    }
}
