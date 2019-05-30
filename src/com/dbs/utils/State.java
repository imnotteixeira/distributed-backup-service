package com.dbs.utils;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class State implements Serializable {

    private int maxSpace;
    private ConcurrentHashMap<String, Integer> storedFiles;

    State() {
        this.maxSpace = Integer.MAX_VALUE;
    }

    public void setMaxSpace(int maxSpace) {
        this.maxSpace = maxSpace;
    }

    private int getSpace() {
        int space = 0;
        for (Integer fileSize : storedFiles.values()) {
            space += fileSize;
        }

        return space;
    }

    public boolean hasSpace(int fileSize) {
        return this.maxSpace > (fileSize + this.getSpace());
    }

    public boolean addFile(String id, int size) {
        if (hasSpace(size)) {
            storedFiles.put(id, size);
            return true;
        } else {
            return false;
        }
    }

    public boolean hasFile(String id) {
        return this.storedFiles.containsKey(id);
    }

    public void deleteFile(String id) {
        if (hasFile(id)) {
            storedFiles.remove(id);
        }
    }

    private String storedFilesString() {
        StringBuilder files = new StringBuilder();

        if (storedFiles.isEmpty()) {
            return "No stored chunks.\n";
        }

        for (Map.Entry<String, Integer> file : storedFiles.entrySet()) {
            files.append("Chunk Id: ").append(file.getKey()).append("\n");
            files.append("Size: ").append(file.getValue()).append("\n");
        }

        return files.toString();
    }

    @Override
    public String toString() {
        String state;
        state = "[STATE]\n";
        state += "\nBacked up files:\n";
        state += storedFilesString();
        state += "\nUsed space: " +  this.getSpace();
        state += "\nMax space: " + this.maxSpace + "\n";
        return state;
    }
}
