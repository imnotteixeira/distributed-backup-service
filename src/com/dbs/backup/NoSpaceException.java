package com.dbs.backup;

public class NoSpaceException extends Exception{
    public NoSpaceException(){
        super("Not enough space");
    }
}
