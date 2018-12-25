package com.sndo.dmp.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.sndo.dmp.CellScanner;

import java.io.IOException;

public class Call {

    final int id;   // call id
    final Message param;
    CellScanner cells;
    Message response;
    Message responseDefaultType;
    IOException error;
    volatile boolean done;
    final Descriptors.MethodDescriptor method;
    final int timeout;


    Call(int id, final Descriptors.MethodDescriptor method, Message param, final CellScanner cells,
         final Message responseDefaultType, int timeout) {
        this.id = id;
        this.method = method;
        this.param = param;
        this.cells = cells;
        this.responseDefaultType = responseDefaultType;
        this.timeout = timeout;
    }



}
