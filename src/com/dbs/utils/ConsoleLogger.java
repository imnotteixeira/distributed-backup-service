package com.dbs.utils;

import com.dbs.chord.SimpleNodeInfo;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConsoleLogger {


    private static Logger logger;

    public static void bootstrap(SimpleNodeInfo info) {
        logger = Logger.getLogger(ConsoleLogger.class.getName());
        logger.setUseParentHandlers(false);

        CustomFormatter formatter = new CustomFormatter(info);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(formatter);
        logger.addHandler(handler);

    }

    public static void log(Level level, String msg) {
        logger.log(level, msg);
    }
}
