package com.sndo.dmp.exceptions;

public class FatalConnectionException extends DoNotRetryIOException {

    public FatalConnectionException() {
        super();
    }

    public FatalConnectionException(String message) {
        super(message);
    }

    public FatalConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
