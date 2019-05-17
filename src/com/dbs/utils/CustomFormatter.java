package com.dbs.utils;

import com.dbs.chord.SimpleNodeInfo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

public class CustomFormatter extends Formatter {
    // Create a DateFormat to format the logger timestamp.
    private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
    // ANSI escape code
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    SimpleNodeInfo nodeInfo;

    public CustomFormatter(SimpleNodeInfo info) {
        this.nodeInfo = info;
    }

    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        builder.append(ANSI_PURPLE);
        builder.append(nodeInfo.address).append(":").append(nodeInfo.port).append(" - ");
        builder.append(df.format(new Date(record.getMillis()))).append(" - ");
        builder.append(levelColor(record.getLevel()));
        builder.append("[").append(record.getLevel()).append("] - ");
        builder.append(formatMessage(record));
        builder.append(ANSI_RESET).append("\n");
        return builder.toString();
    }

    private String levelColor(Level level) {
        if(level.equals(Level.INFO)) {
            return ANSI_BLUE;
        }

        if (level.equals(Level.WARNING)) {
            return ANSI_YELLOW;
        }

        if(level.equals(Level.FINE)) {
            return ANSI_GREEN;
        }

        if(level.equals(Level.SEVERE)) {
            return ANSI_RED;
        }

        return ANSI_WHITE;
    }


    public String getHead(Handler h) {
        return super.getHead(h);
    }

    public String getTail(Handler h) {
        return super.getTail(h);
    }
}
