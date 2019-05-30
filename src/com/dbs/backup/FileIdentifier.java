package com.dbs.backup;

import java.io.Serializable;
import java.util.Objects;

public class FileIdentifier implements Serializable {

    String fileName, creationTime;

    long fileSize;

    public FileIdentifier(String fileName, String creationTime, long fileSize) {
        this.fileName = fileName;
        this.creationTime = creationTime;
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public String getCreationTime() {
        return creationTime;
    }

    public long getFileSize() {
        return fileSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FileIdentifier)) return false;
        FileIdentifier that = (FileIdentifier) o;
        return fileSize == that.fileSize &&
                Objects.equals(fileName, that.fileName) &&
                Objects.equals(creationTime, that.creationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, creationTime, fileSize);
    }
}
