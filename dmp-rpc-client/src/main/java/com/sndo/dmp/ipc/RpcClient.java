package com.sndo.dmp.ipc;

import com.google.protobuf.BlockingRpcChannel;
import com.sndo.dmp.ServerName;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author yangqi
 * @date 2018/12/18 15:18
 **/
public interface RpcClient extends Closeable {

    public final static int DEFAULT_SOCKET_TIMEOUT_CONNECT = 10000; // 10 seconds
    public final static int DEFAULT_SOCKET_TIMEOUT_READ = 20000; // 20 seconds
    public final static int DEFAULT_SOCKET_TIMEOUT_WRITE = 60000; // 60 seconds

    public BlockingRpcChannel createBlockingRpcChannel(ServerName serverName, int rpcTimeout);

    public void cancleConnections(ServerName serverName);

    @Override
    void close() throws IOException;


}
