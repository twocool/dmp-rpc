package com.sndo.dmp.exceptions;

import org.apache.hadoop.hbase.HBaseIOException;

public class FailedServerException extends HBaseIOException {

    public FailedServerException(String message) {
        super(message);
    }
}
