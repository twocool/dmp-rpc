package com.sndo.dmp.ipc;

import java.io.IOException;

public class CallTimeoutException extends IOException {

    public CallTimeoutException(final String message) {
        super(message);
    }

}
