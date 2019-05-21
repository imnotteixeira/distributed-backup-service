package com.dbs.chord;

import com.dbs.utils.ConsoleLogger;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.logging.Level;

public class Utils {
    public static boolean between(BigInteger middle, BigInteger left, BigInteger right) {
        return after(middle, left) && before(middle, right);
    }

    public static boolean after(BigInteger b1, BigInteger b2) {

        BigInteger b1_mod = b1.mod(BigInteger.valueOf(2).pow(Chord.NUM_BITS_KEYS));
        BigInteger b2_mod = b2.mod(BigInteger.valueOf(2).pow(Chord.NUM_BITS_KEYS));


        return b1_mod.compareTo(b2_mod) > 0;
    }

    public static boolean before(BigInteger b1, BigInteger b2) {

        BigInteger b1_mod = b1.mod(BigInteger.valueOf(2).pow(Chord.NUM_BITS_KEYS));
        BigInteger b2_mod = b2.mod(BigInteger.valueOf(2).pow(Chord.NUM_BITS_KEYS));

        return b1_mod.compareTo(b2_mod) < 0;
    }

    public static SSLSocket createClientSocket(InetAddress destAddress, int destPort) throws IOException {
        return (SSLSocket) SSLSocketFactory.getDefault().createSocket(destAddress, destPort);
    }
}
