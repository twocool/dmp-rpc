package com.sndo.dmp.exceptions;

public class UnsupportedCellCodecException extends FatalConnectionException {

    public UnsupportedCellCodecException() {
        super();
    }

    public UnsupportedCellCodecException(String message) {
        super(message);
    }

    public UnsupportedCellCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
