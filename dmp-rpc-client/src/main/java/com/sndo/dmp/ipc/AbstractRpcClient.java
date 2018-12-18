package com.sndo.dmp.ipc;

import com.google.protobuf.*;
import com.sndo.dmp.CellScanner;
import com.sndo.dmp.Constants;
import com.sndo.dmp.ServerName;
import com.sndo.dmp.conf.Configuration;
import com.sndo.dmp.util.EnviromentEdgeManager;
import com.sndo.dmp.util.Pair;
import com.sndo.dmp.util.PoolMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author yangqi
 * @date 2018/12/18 15:26
 **/
public abstract class AbstractRpcClient implements RpcClient {

    private static final Log LOG = LogFactory.getLog(AbstractRpcClient.class);

    private final Configuration conf;
    private final SocketAddress localAddr;

    private final int minIdleTimeBeforeClose;
    private final int maxRetries;
    private final long failureSleep;

    private final boolean tcpNoDelay;
    private final boolean tcpKeepAlive;

    private final int connectTimeout;
    private final int readTimeout;
    private final int writeTimeout;

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
    }

    protected static PoolMap.PoolType getPoolType(Configuration config) {
        return PoolMap.PoolType
                .valueOf(config.get(Constants.CLIENT_IPC_POOL_TYPE), PoolMap.PoolType.RoundRobin,
                        PoolMap.PoolType.ThreadLocal);
    }

    protected static int getPoolSize(Configuration config) {
        return config.getInt(Constants.CLIENT_IPC_POOL_SIZE, 1);
    }

    // TODO
    private Message callBlockingMethod(Descriptors.MethodDescriptor method, PayloadCarryingRpcController pcrc,
                                       Message param, Message returnType, final InetSocketAddress isa) throws ServiceException {
        if (pcrc == null) {
            pcrc = new PayloadCarryingRpcController();
        }

        Pair<Message, CellScanner> val;
        try {
            long begin = EnviromentEdgeManager.currentTime();

            val = call(pcrc, method, param, returnType, isa);
//            pcrc.setCellScanner(val.getSecond()); // TODO
            if (LOG.isTraceEnabled()) {
                LOG.trace("Call: " + method.getName() + ", callTime: " + (EnviromentEdgeManager.currentTime() - begin) + "ms");
            }
            return val.getFirst();
        } catch (Throwable e) {
            throw new ServiceException(e);
        }
    }

    protected abstract Pair<Message, CellScanner> call(PayloadCarryingRpcController pcrc, Descriptors.MethodDescriptor method,
                                                       Message param, Message returnType, InetSocketAddress isa);

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
            this.isa = new InetSocketAddress(sn.getServername(), sn.getPort());
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
