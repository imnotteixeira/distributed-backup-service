package com.dbs.network;

import com.dbs.network.messages.ChordMessage;
import com.dbs.utils.ConsoleLogger;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Level;

public class Communicator {


    private SSLServerSocket serverSocket = null;


    public Communicator(SSLServerSocket s) {
        this.serverSocket = s;
    }

    public void send(SSLSocket s, ChordMessage msg) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
        out.writeObject(msg);

        s.close();
        ConsoleLogger.log(Level.SEVERE, "Sent message " + msg + " to " + s.getInetAddress().getHostAddress() + ":" + s.getPort() +  "... ");
    }

    public Object receive() throws IOException, ClassNotFoundException {

        ConsoleLogger.log(Level.SEVERE, this + " communicator.receive(), will call accept, which blocks");

        SSLSocket clientSocket = (SSLSocket) serverSocket.accept();
        ConsoleLogger.log(Level.SEVERE, this + " accept() caught something");

        ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        Object o = in.readObject();

        ConsoleLogger.log(Level.SEVERE, "Read object inside communicator receive "+ o.getClass());

        return o;
    }

    public void setServerSocket(SSLServerSocket serverSocket) {
        this.serverSocket = serverSocket;
    }

    public int getPort() {
        return this.serverSocket.getLocalPort();
    }
}
