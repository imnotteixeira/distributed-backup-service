package com.dbs.filemanager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.*;

public class FileManager {

    public static Future<Integer> readFromFile(String filePath, ByteBuffer data, long filePosition) {

        Path path = Paths.get(filePath);
        AsynchronousFileChannel fileChannel;
        int nBytes;

        try {
            fileChannel = AsynchronousFileChannel.open(path, READ);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return fileChannel.read(data, filePosition);
    }

    public static int deleteFile(String filePath) {

        Path path = Paths.get(filePath);

        try {
            Files.delete(path);
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static Future<Integer> writeToFile(String filePath, ByteBuffer data, long filePosition) {

        Path path = Paths.get(filePath);
        AsynchronousFileChannel fileChannel;
        int nBytes;

        try {
            fileChannel = AsynchronousFileChannel.open(path, WRITE, CREATE);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        return fileChannel.write(data, filePosition);
    }

    /**
     *
     * @param directoryName directory Name
     * @return Returns 0 on success and -1 if directory already exists
     */
    public static int createDirectory(String directoryName) {

        try {
            Files.createDirectory(Paths.get(directoryName));
        } catch (IOException e) {
            return -1;
        }

        return 0;
    }
}
