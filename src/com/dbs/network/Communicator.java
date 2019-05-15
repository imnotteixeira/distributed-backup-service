package com.dbs.network;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Communicator {


    private ServerSocket serverSocket = null;


    public Communicator(ServerSocket s) {
        this.serverSocket = s;
    }

    public void send(Socket s, String msg) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
        out.writeObject(msg);
        System.out.println("Sending message to " +s.getPort() +  "... ");
        System.out.println("Content: " + msg);
    }

    public Object receive() throws IOException, ClassNotFoundException {

        Socket clientSocket = serverSocket.accept();
        ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        Object o = in.readObject();

        return o;
    }

    public void setServerSocket(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
    }
}
