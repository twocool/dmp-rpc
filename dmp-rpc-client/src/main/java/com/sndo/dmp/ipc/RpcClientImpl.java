package com.sndo.dmp.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.sndo.dmp.CellScanner;
import com.sndo.dmp.ServerName;
import com.sndo.dmp.conf.Configuration;
import com.sndo.dmp.util.Pair;
import com.sndo.dmp.util.PoolMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yangqi
 * @date 2018/12/18 17:20
 **/
public class RpcClientImpl extends AbstractRpcClient {

    private static final Log LOG = LogFactory.getLog(RpcClientImpl.class);
    private final SocketFactory socketFactory;
    private final PoolMap<ConnectionId, Connection> connections;

    private final AtomicInteger callIdCnt = new AtomicInteger();

    private final AtomicBoolean running = new AtomicBoolean(true);

    public RpcClientImpl(Configuration conf, SocketFactory factory, SocketAddress localAddr) {
        super(conf, localAddr);

        this.socketFactory = factory;
        this.connections = new PoolMap<ConnectionId, Connection>(getPoolType(conf), getPoolSize(conf));
//        this.failedServers = new FailedServers(conf);
    }

    @Override
    public void cancleConnections(ServerName serverName) {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    protected Pair<Message, CellScanner> call(PayloadCarryingRpcController pcrc,
                                              Descriptors.MethodDescriptor method,
                                              Message param, Message returnType,
                                              InetSocketAddress addr) {
        if (pcrc == null) {
            pcrc = new PayloadCarryingRpcController();
        }
        CellScanner cells = pcrc.cellScanner();

        final Call call = new Call(this.callIdCnt.getAndIncrement(), method, param, cells, returnType, pcrc.getCallTimeout());

        return null;
    }


    private Connection getConnection(Call call, InetSocketAddress addr) throws IOException {
        if (!running.get()) {
            throw new StoppedRpcClientException();
        }
        Connection connection;

        return null;
    }

    // TODO
    private class Connection extends Thread {

        @Override
        public void run() {
        }

    }

}
