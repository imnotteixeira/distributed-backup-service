package com.dbs.network;

import com.dbs.chord.NodeInfo;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class NullNodeInfo extends NodeInfo {
    public NullNodeInfo() throws NoSuchAlgorithmException, IOException {
        super(null, -1);
    }
}
