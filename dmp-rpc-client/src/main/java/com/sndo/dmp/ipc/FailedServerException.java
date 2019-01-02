package com.sndo.dmp.ipc;

import org.apache.hadoop.hbase.HBaseIOException;

public class FailedServerException extends HBaseIOException {

    public FailedServerException(String message) {
        super(message);
    }
}
