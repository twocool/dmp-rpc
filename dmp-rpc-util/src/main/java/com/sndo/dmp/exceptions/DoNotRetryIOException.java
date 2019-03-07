package com.sndo.dmp.exceptions;

import org.apache.hadoop.hbase.HBaseIOException;

public class DoNotRetryIOException extends HBaseIOException {
    private static final long serialVersionUID = -6950539729309980796L;

    public DoNotRetryIOException() {
        super();
    }

    public DoNotRetryIOException(String message) {
        super(message);
    }

    public DoNotRetryIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public DoNotRetryIOException(Throwable cause) {
        super(cause);
    }

}
