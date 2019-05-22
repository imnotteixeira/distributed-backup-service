package com.dbs.chord;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;

public interface Chord {

    int NUM_BITS_KEYS = 64;

    NodeInfo findSuccessor(BigInteger key) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException;

    void create();
    void join(NodeInfo existingNode) throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException;
    void notify(NodeInfo successor) throws IOException, NoSuchAlgorithmException;
    void handlePredecessorNotification(SimpleNodeInfo predecessor) throws IOException, NoSuchAlgorithmException;

    void stabilize() throws IOException, InterruptedException, NoSuchAlgorithmException, ExecutionException;

    void handleSuccessorFail() throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException;

    void fixFingers() throws InterruptedException, ExecutionException, NoSuchAlgorithmException, IOException;

    void checkPredecessor() throws IOException, NoSuchAlgorithmException, ExecutionException, InterruptedException;
}
