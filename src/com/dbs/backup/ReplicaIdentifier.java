package com.dbs.backup;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Objects;

public class ReplicaIdentifier implements Serializable {

    FileIdentifier fileId;
    BigInteger hash;

    public ReplicaIdentifier(FileIdentifier fileId, BigInteger hash){
        this.fileId = fileId;
        this.hash = hash;
    }

    public FileIdentifier getFileId() {
        return fileId;
    }

    public BigInteger getHash() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReplicaIdentifier)) return false;
        ReplicaIdentifier that = (ReplicaIdentifier) o;
        return Objects.equals(fileId, that.fileId) &&
                Objects.equals(hash, that.hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hash);
    }
}
