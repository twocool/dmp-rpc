package com.sndo.dmp.ipc;

import com.google.protobuf.*;
import com.sndo.dmp.ServerName;
import com.sndo.dmp.client.MetricsConnection;
import com.sndo.dmp.exceptions.ConnectionClosingException;
import com.sndo.dmp.util.PoolMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;

public abstract class AbstractRpcClient implements RpcClient {

    private static final Log LOG = LogFactory.getLog(AbstractRpcClient.class);

    protected final Configuration conf; // 服务配置类
    protected final SocketAddress localAddr;    // 服务地址

    protected final int minIdleTimeBeforeClose; // 空闲最大时长
    protected final int maxRetries; // 连接最大尝试次数
    protected final long failureSleep;  // 失败休眠时间

    protected final boolean tcpNoDelay; // tcp是否delay
    protected final boolean tcpKeepAlive;   // tcp是否keep alive

    protected final int connectTimeout; // 连接超时时长
    protected final int readTimeout;    // 读取超时时长
    protected final int writeTimeout;   // 写入超时时长

    protected final Codec codec;    // 编码
    protected final CompressionCodec compressor;    // 压缩器

    protected final IPCUtil ipcUtil;    // IPC(Inter-Process Communication: 进程间通信)工具类

    public AbstractRpcClient(Configuration conf, SocketAddress localAddr) {
        this.conf = conf;

        this.localAddr = localAddr;

        this.tcpKeepAlive = true;
        this.tcpNoDelay = true;

        this.failureSleep = 100;
        this.maxRetries = 0;
        this.minIdleTimeBeforeClose = 2 * 60 * 1000; // 2min

        this.connectTimeout = DEFAULT_SOCKET_TIMEOUT_CONNECT;
        this.readTimeout = DEFAULT_SOCKET_TIMEOUT_READ;
        this.writeTimeout = DEFAULT_SOCKET_TIMEOUT_WRITE;

        this.codec = getCodec();    // 默认为空
        this.compressor = getCompressor();  // 默认为空

        this.ipcUtil = new IPCUtil(conf);
    }

    // 获取编码类
    Codec getCodec() {
        // For NO CODEC, "hbase.client.rpc.codec" must be configured with empty string AND
        // "hbase.client.default.rpc.codec" also -- because default is to do cell block encoding.
        String className = conf.get(HConstants.RPC_CODEC_CONF_KEY, getDefaultCodec(this.conf));
        if (className == null || className.length() == 0) return null;
        try {
            return (Codec)Class.forName(className).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed getting codec " + className, e);
        }
    }

    // 获取默认codec_class
    public static String getDefaultCodec(final Configuration c) {
        // If "hbase.client.default.rpc.codec" is empty string -- you can't set it to null because
        // Configuration will complain -- then no default codec (and we'll pb everything).  Else
        // default is KeyValueCodec
        return c.get(DEFAULT_CODEC_CLASS, KeyValueCodec.class.getCanonicalName());
    }

    // 获取压缩器类
    private CompressionCodec getCompressor() {
        String className = conf.get("hbase.client.rpc.compressor", null);
        if (className == null || className.isEmpty()) return null;
        try {
            return (CompressionCodec)Class.forName(className).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed getting compressor " + className, e);
        }
    }

    protected static PoolMap.PoolType getPoolType(Configuration config) {
        return PoolMap.PoolType
                .valueOf(config.get(HConstants.HBASE_CLIENT_IPC_POOL_TYPE), PoolMap.PoolType.RoundRobin,
                        PoolMap.PoolType.ThreadLocal);
    }

    protected static int getPoolSize(Configuration config) {
        return config.getInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, 1);
    }

    // 封装Exception
    protected IOException wrapException(InetSocketAddress addr, Exception exception) {
        if (exception instanceof ConnectException) {
            // connection refused; include the host:port in the error
            return (ConnectException) new ConnectException("Call to " + addr
                    + " failed on connection exception: " + exception).initCause(exception);
        } else if (exception instanceof SocketTimeoutException) {
            return (SocketTimeoutException) new SocketTimeoutException("Call to " + addr
                    + " failed because " + exception).initCause(exception);
        } else if (exception instanceof ConnectionClosingException) {
            return (ConnectionClosingException) new ConnectionClosingException("Call to " + addr
                    + " failed on local exception: " + exception).initCause(exception);
        } else {
            return (IOException) new IOException("Call to " + addr + " failed on local exception: "
                    + exception).initCause(exception);
        }
    }

    private Message callBlockingMethod(Descriptors.MethodDescriptor method, PayloadCarryingRpcController pcrc,
                                       Message param, Message returnType, final InetSocketAddress isa) throws ServiceException {
        if (pcrc == null) {
            pcrc = new PayloadCarryingRpcController();
        }

        Pair<Message, CellScanner> val; // 返回值
        try {
            final MetricsConnection.CallStats callStats = MetricsConnection.callStats();    // call状态
            callStats.setStartTime(EnvironmentEdgeManager.currentTime());   // 设置开始时间
            val = call(pcrc, method, param, returnType, isa, callStats);    // 调用服务
            pcrc.setCellScanner(val.getSecond());
            callStats.setCallTimeMs(EnvironmentEdgeManager.currentTime() - callStats.getStartTime());   // 调用服务耗时; 单位: 毫秒
            if (LOG.isTraceEnabled()) {
                LOG.trace("Call: " + method.getName() + ", callTime: " + (EnvironmentEdgeManager.currentTime() - callStats.getStartTime()) + "ms");
            }
            return val.getFirst();
        } catch (Throwable e) {
            throw new ServiceException(e);
        }
    }

    protected abstract Pair<Message, CellScanner> call(PayloadCarryingRpcController pcrc, Descriptors.MethodDescriptor method,
                                                       Message param, Message returnType, InetSocketAddress isa, MetricsConnection.CallStats callStats) throws IOException, InterruptedException;

    @Override
    public BlockingRpcChannel createBlockingRpcChannel(ServerName serverName, int rpcTimeout) {
        return new BlockingRpcChannelImplementation(this, serverName, rpcTimeout);
    }

    private class BlockingRpcChannelImplementation implements BlockingRpcChannel {
        private final InetSocketAddress isa;
        private final AbstractRpcClient rpcClient;
        private final int channelOperateTimeout;

        public BlockingRpcChannelImplementation(final AbstractRpcClient rpcClient,
                                                final ServerName sn,
                                                int channelOperationTimeout) {
            this.isa = new InetSocketAddress(sn.getHostName(), sn.getPort());
            this.rpcClient = rpcClient;
            this.channelOperateTimeout = channelOperationTimeout;
        }

        @Override
        public Message callBlockingMethod(Descriptors.MethodDescriptor method, RpcController controller,
                                          Message param, Message returnType) throws ServiceException {
            PayloadCarryingRpcController pcrc = null;
            if (controller != null) {
                pcrc = (PayloadCarryingRpcController) controller;
                if (!pcrc.hasCallTimeout()) {
                    pcrc.setCallTimeout(channelOperateTimeout);
                }
            } else {
                pcrc = new PayloadCarryingRpcController();
                pcrc.setCallTimeout(channelOperateTimeout);
            }

            return rpcClient.callBlockingMethod(method, pcrc, param, returnType, isa);
        }
    }
}
