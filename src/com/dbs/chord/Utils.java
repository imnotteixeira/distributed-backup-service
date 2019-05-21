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
       if(right.compareTo(left) > 0){
           return middle.compareTo(left) > 0 && middle.compareTo(right) < 0;
       }else if(right.compareTo(left) < 0){
           return middle.compareTo(left) > 0 || middle.compareTo(right) < 0;
       }
       return false;
    }

    public static SSLSocket createClientSocket(InetAddress destAddress, int destPort) throws IOException {
        return (SSLSocket) SSLSocketFactory.getDefault().createSocket(destAddress, destPort);
    }
}
