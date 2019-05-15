package com.dbs.utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ByteToHash {
    public static BigInteger convert(byte[] bytes, String HASH_ALGORITHM) throws NoSuchAlgorithmException {

        return new BigInteger(1, MessageDigest.getInstance(HASH_ALGORITHM).digest(bytes));

    }
}
