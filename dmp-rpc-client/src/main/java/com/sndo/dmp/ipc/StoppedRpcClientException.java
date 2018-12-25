package com.sndo.dmp.ipc;

import com.sndo.dmp.HBaseIOException;

public class StoppedRpcClientException extends HBaseIOException {

    public StoppedRpcClientException() {
        super();
    }

    public StoppedRpcClientException(String message) {
        super(message);
    }
}
