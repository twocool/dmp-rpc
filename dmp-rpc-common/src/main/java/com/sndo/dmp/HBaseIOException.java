package com.sndo.dmp;

import java.io.IOException;

public class HBaseIOException extends IOException {

    public HBaseIOException() {
        super();
    }

    public HBaseIOException(String message) {
        super(message);
    }

    public HBaseIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseIOException(Throwable cause) {
        super(cause);
    }

}
