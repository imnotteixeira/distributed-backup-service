package com.dbs.filemanager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.*;

public class FileManager {

    public static int readFromFile(String filePath, ByteBuffer data, long filePosition) {

        Path path = Paths.get(filePath);
        AsynchronousFileChannel fileChannel;
        int nBytes;

        try {
            fileChannel = AsynchronousFileChannel.open(path, READ);
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        Future<Integer> result = fileChannel.read(data, filePosition);

        try {
            nBytes = result.get();
            return nBytes;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return -1;
        }

    }

    public static int writeToFile(String filePath, ByteBuffer data, long filePosition) {

        Path path = Paths.get(filePath);
        AsynchronousFileChannel fileChannel;
        int nBytes;

        try {
            fileChannel = AsynchronousFileChannel.open(path, WRITE, CREATE);
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        Future<Integer> result =  fileChannel.write(data, filePosition);

        try {
            nBytes = result.get();
            return nBytes;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static void main(String[] args) {
        ByteBuffer data = ByteBuffer.wrap("cenascenascenas".getBytes());
        System.out.println(FileManager.writeToFile("out.txt", data, 0));

        ByteBuffer data1 = ByteBuffer.allocate(1024);
        System.out.println(FileManager.readFromFile("out.txt", data1, 0));
        System.out.println(new String(data1.array()).trim());
    }

}
