package com.dbs.network;

import java.io.IOException;

public class Listener {


    public static void listen(Communicator communicator) {
        while(true) {
            try {
                System.out.println("Listening for messages...");
                Object o = communicator.receive();

                System.out.println("Received an Object:");
                System.out.println((String) o);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
