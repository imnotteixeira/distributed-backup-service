package com.dbs.backup;

import java.math.BigInteger;

public class FileIdentifier {

    String originalPath, creationTime;
    BigInteger replicaHash;

    public FileIdentifier(String originalPath, String creationTime, BigInteger replicaHash){
        this.originalPath = originalPath;
        this.creationTime = creationTime;
        this.replicaHash = replicaHash;
    }

    public BigInteger getHash() {
        return replicaHash;
    }
}
