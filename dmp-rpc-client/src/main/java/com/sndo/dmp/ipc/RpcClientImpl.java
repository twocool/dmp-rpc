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

/**
 * @author yangqi
 * @date 2018/12/18 17:20
 **/
public class RpcClientImpl extends AbstractRpcClient {

    private static final Log LOG = LogFactory.getLog(RpcClientImpl.class);
    private final SocketFactory socketFactory;
    private final PoolMap<ConnectionId, Connection> connections;

    RpcClientImpl(Configuration conf, SocketFactory factory, SocketAddress localAddr) {
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
                                              InetSocketAddress isa) {
        return null;
    }

    // TODO
    private class Connection extends Thread {

        @Override
        public void run() {
        }

    }

}
