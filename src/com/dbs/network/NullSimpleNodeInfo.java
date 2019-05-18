package com.dbs.network;

import com.dbs.chord.NodeInfo;
import com.dbs.chord.SimpleNodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class NullSimpleNodeInfo extends SimpleNodeInfo {
    public NullSimpleNodeInfo() throws IOException, NoSuchAlgorithmException {
        super(new NullNodeInfo());
    }
}
