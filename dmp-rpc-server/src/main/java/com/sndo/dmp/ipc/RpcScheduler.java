package com.sndo.dmp.ipc;

import java.net.InetSocketAddress;

public abstract class RpcScheduler {

    static abstract class Context {
        public abstract InetSocketAddress getListenerAddress();
    }

    public abstract void init(Context context);

    public abstract void start();

    public abstract void stop();

    public abstract void dispatch(CallRunner task) throws InterruptedException;

}
