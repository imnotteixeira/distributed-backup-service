package com.dbs.utils;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Network {

    private static final String TEST_CHANNEL = "239.0.0.0";

    public static InetAddress getSelfAddress(int port) throws ExecutionException, InterruptedException {

        CompletableFuture<InetAddress> wait = new CompletableFuture<>();
        try {
            InetAddress multicastAddr = InetAddress.getByName(TEST_CHANNEL);
            MulticastSocket multicastSocket = new MulticastSocket(port);
            multicastSocket.joinGroup(multicastAddr);

            byte[] msgToSend = "1234567890".getBytes();

            wait = CompletableFuture.supplyAsync(() -> {


                DatagramPacket packet = new DatagramPacket(new byte[msgToSend.length], msgToSend.length);

                do {
                    try {
                        multicastSocket.send(new DatagramPacket(msgToSend, msgToSend.length, multicastAddr, port));
                        multicastSocket.receive(packet);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } while(Arrays.copyOf(packet.getData(), packet.getLength()).equals(Arrays.copyOf(msgToSend, msgToSend.length)));

                multicastSocket.close();
                return packet.getAddress();
            });


        } catch (IOException e) {
            e.printStackTrace();
        }

        return wait.get();

    }
}
