package com.dbs.filemanager;

import com.dbs.utils.ByteToHash;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.*;

public class FileManager {

    public static byte[] readFromFile(String filePath) throws ExecutionException, InterruptedException, FileNotFoundException {

        Path path = Paths.get(filePath);
        AsynchronousFileChannel fileChannel;
        int nBytes;

        try {
            fileChannel = AsynchronousFileChannel.open(path, READ);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        File file = new File(filePath);
        if(!file.exists()) {
            throw new FileNotFoundException();
        }

        long fileSize = file.length();

        ByteBuffer data = ByteBuffer.allocate((int) fileSize);

        fileChannel.read(data, 0).get();

        data.rewind();
        byte[] arr = new byte[data.remaining()];
        data.get(arr);

        return arr;
    }

    public static void deleteFile(String filePath) throws IOException {

        Path path = Paths.get(filePath);

        Files.delete(path);

    }

    public static void writeToFile(String filePath, byte[] data) throws IOException, ExecutionException, InterruptedException {

        Path path = Paths.get(filePath);
        AsynchronousFileChannel fileChannel;

        fileChannel = AsynchronousFileChannel.open(path, WRITE, CREATE);


        ByteBuffer buffer = ByteBuffer.allocate(data.length);

        buffer.put(data);
        buffer.flip();

        Future<Integer> operation = fileChannel.write(buffer, 0);
        buffer.clear();

        //run other code as operation continues in background
        operation.get();
    }

    /**
     *
     * @param directoryName directory Name
     * @return Returns 0 on success and -1 if directory already exists
     */
    public static Path createDirectory(String directoryName) throws IOException {

        try {

            return Files.createDirectory(Paths.get(directoryName));

        } catch (FileAlreadyExistsException e ) {
            return Paths.get(directoryName);
        }
    }

    public static BigInteger[] generateFileIds(String filePath) throws IOException, NoSuchAlgorithmException {
        File file = new File(filePath);

        String name = file.getName();

        BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class);

        String creationTime = attr.creationTime().toString();

        ByteBuffer rawId = ByteBuffer.allocate(name.getBytes().length + creationTime.getBytes().length + Integer.BYTES)
                .put(name.getBytes())
                .put(creationTime.getBytes());

        byte[] rawId0 = rawId.duplicate().putInt(0).array();
        byte[] rawId1 = rawId.duplicate().putInt(1).array();
        byte[] rawId2 = rawId.duplicate().putInt(2).array();

        BigInteger[] ids = new BigInteger[3];

        ids[0] = ByteToHash.convert(rawId0, "SHA-256");
        ids[1] = ByteToHash.convert(rawId0, "SHA-256");
        ids[2] = ByteToHash.convert(rawId0, "SHA-256");

        return ids;
    }
}
