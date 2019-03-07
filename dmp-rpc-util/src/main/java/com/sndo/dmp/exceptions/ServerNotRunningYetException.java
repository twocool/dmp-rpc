package com.sndo.dmp.exceptions;

import java.io.IOException;

public class ServerNotRunningYetException extends IOException {
    public ServerNotRunningYetException(String message) {
        super(message);
    }
}
