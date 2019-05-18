package com.dbs.network.messages;


import com.dbs.chord.Node;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

/**
 * Contains info about a specific node
 */
public abstract class NodeInfoMessage extends ChordMessage{

    SimpleNodeInfo node;

    public NodeInfoMessage(MESSAGE_TYPE type, SimpleNodeInfo node) {
        super(type);
        this.node = node;
    }

    public SimpleNodeInfo getNode() {
        return node;
    }
}
