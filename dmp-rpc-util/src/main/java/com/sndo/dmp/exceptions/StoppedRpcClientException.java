package com.sndo.dmp.exceptions;

import org.apache.hadoop.hbase.HBaseIOException;

public class StoppedRpcClientException extends HBaseIOException {

    public StoppedRpcClientException() {
        super();
    }

    public StoppedRpcClientException(String message) {
        super(message);
    }
}
