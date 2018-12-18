package com.sndo.dmp.ipc;

import java.net.InetSocketAddress;

public interface RpcServerInterface {

    void start();

    boolean isStarted();

    void join() throws InterruptedException;

    void stop();

    void setSocketSendBufSize(int size);

    InetSocketAddress getListenerAddress();

    void setErrorHandler(RpcErrorHandler errorHandler);

//    void addCallSize(long diff);

    RpcScheduler getScheduler();

}
