package com.sndo.dmp.exceptions;

import java.io.IOException;

public class ConnectionClosingException extends IOException {
    private static final long serialVersionUID = -8137305982238768143L;

    public ConnectionClosingException(String message) {
        super(message);
    }
}
