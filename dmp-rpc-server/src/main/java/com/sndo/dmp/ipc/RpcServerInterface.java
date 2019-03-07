package com.sndo.dmp.ipc;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface RpcServerInterface {

    void start();

    boolean isStarted();

    void join() throws InterruptedException;

    void stop();

    void setSocketSendBufSize(int size);

    InetSocketAddress getListenerAddress();

    boolean checkOOME(final Throwable e);

//    void setErrorHandler(RpcErrorHandler errorHandler);

//    void addCallSize(long diff);

    RpcScheduler getScheduler();

    Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md, Message param,
                                    CellScanner cellScanner, long receiveTime)
        throws IOException, ServiceException;

}
