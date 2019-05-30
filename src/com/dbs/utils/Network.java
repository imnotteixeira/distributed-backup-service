package com.dbs.utils;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Network {

    private static final String TEST_CHANNEL = "239.0.0.0";

    private static final String[] CIPHER_SUITES = {"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256" };

    private static String[] enabledSuites;

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

    private static void filterCipherSuites() throws NoSuchAlgorithmException {
        SSLContext c = SSLContext.getDefault();
        final Set<String> supportedSuites = new HashSet<>(Arrays.asList(c.getServerSocketFactory().getSupportedCipherSuites()));
        final ArrayList<String> enabledSuites = new ArrayList<>();
        for (String s: CIPHER_SUITES) {
            if (supportedSuites.contains(s))
                enabledSuites.add(s);
        }
        Network.enabledSuites = new String[enabledSuites.size()];
        enabledSuites.toArray(Network.enabledSuites);
    }

    public static SSLServerSocket createServerSocket(int i) throws IOException, NoSuchAlgorithmException {
        if (enabledSuites == null) {
            Network.filterCipherSuites();
        }
        SSLServerSocket socket = (SSLServerSocket) SSLServerSocketFactory.getDefault().createServerSocket(i);
        socket.setEnabledCipherSuites(enabledSuites);
        socket.setNeedClientAuth(true);
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setUseCipherSuitesOrder(true);
        socket.setSSLParameters(parameters);
        return socket;
    }
}
