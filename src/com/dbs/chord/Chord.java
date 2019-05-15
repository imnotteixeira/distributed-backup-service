package com.dbs.chord;

import java.io.IOException;
import java.math.BigInteger;

public interface Chord {
    NodeInfo findSuccessor(BigInteger key);

    void join(NodeInfo successorInfo) throws IOException;
    void notify(NodeInfo successor) throws IOException;
}
