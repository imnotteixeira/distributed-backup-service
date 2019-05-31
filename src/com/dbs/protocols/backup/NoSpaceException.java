package com.dbs.protocols.backup;

public class NoSpaceException extends Exception{
    public NoSpaceException(){
        super("Not enough space");
    }
}
