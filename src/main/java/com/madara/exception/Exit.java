package com.madara.exception;

public class Exit {
    public static void systemExit(Exception error) {
        error.printStackTrace();
        System.exit(1);
    }
}
