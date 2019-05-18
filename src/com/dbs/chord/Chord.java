package com.dbs.chord;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public interface Chord {

    int NUM_BITS_KEYS = 256;

    NodeInfo findSuccessor(BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException;

    void create();
    void join(NodeInfo existingNode) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException;
    void notify(NodeInfo successor) throws IOException;
    void handleSucessorNotification(SimpleNodeInfo predecessor);
}
