package com.sndo.dmp.ipc;

import com.google.protobuf.BlockingService;
import com.sndo.dmp.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.List;

public class RpcServer implements RpcServerInterface {

    private Log LOG = LogFactory.getLog(RpcServer.class);

    private RpcServer(final String name,
                      final List<BlockingServiceAndInterface> services,
                      final InetSocketAddress bingAddress, Configuration conf,
                      RpcScheduler scheduler) {

    }

    @Override
    public void start() {

    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public void stop() {

    }

    @Override
    public void setSocketBufSize(int size) {

    }

    @Override
    public InetSocketAddress getListenerAddress() {
        return null;
    }

    @Override
    public void setErrorHandler(RpcErrorHandler errorHandler) {

    }

    @Override
    public RpcScheduler getScheduler() {
        return null;
    }

    public static class BlockingServiceAndInterface {
        private final BlockingService service;
        private final Class<?> serviceInterface;

        public BlockingServiceAndInterface(final BlockingService service,
                                           final Class<?> serviceInterface) {
            this.service = service;
            this.serviceInterface = serviceInterface;
        }

        public Class<?> getServiceInterface() {
            return this.serviceInterface;
        }

        public BlockingService getService() {
            return this.service;
        }
    }
}
