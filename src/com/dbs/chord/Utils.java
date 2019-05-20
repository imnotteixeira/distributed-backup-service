package com.dbs.chord;

import com.dbs.utils.ConsoleLogger;

import java.math.BigInteger;
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
}
