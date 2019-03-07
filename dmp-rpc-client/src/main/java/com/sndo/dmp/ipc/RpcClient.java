package com.sndo.dmp.ipc;

import com.google.protobuf.BlockingRpcChannel;
import com.sndo.dmp.ServerName;

import java.io.Closeable;
import java.io.IOException;

public interface RpcClient extends Closeable {

    public final static String FAILED_SERVER_EXPIRY_KEY = "hbase.ipc.client.failed.servers.expiry"; // 失效服务有效时间
    public final static int FAILED_SERVER_EXPIRY_DEFAULT = 2000;    // 失效服务有效时间默认值: 2秒

    public static final String SPECIFIC_WRITE_THREAD = "hbase.ipc.client.specificThreadForWriting";

    public static final String DEFAULT_CODEC_CLASS = "hbase.client.default.rpc.codec";

    public final static int DEFAULT_SOCKET_TIMEOUT_CONNECT = 10000; // 10 seconds   // 连接超时
    public final static int DEFAULT_SOCKET_TIMEOUT_READ = 20000; // 20 seconds  // 读超时
    public final static int DEFAULT_SOCKET_TIMEOUT_WRITE = 60000; // 60 seconds // 写超时

    public BlockingRpcChannel createBlockingRpcChannel(ServerName serverName, int rpcTimeout);

    // 取消服务连接
    public void cancelConnections(ServerName serverName);

    @Override
    void close() throws IOException;


}
