package com.sndo.dmp.ipc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.BlockingService;
import com.sndo.dmp.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.*;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RpcServer implements RpcServerInterface {

    private Log LOG = LogFactory.getLog(RpcServer.class);

    private final List<BlockingServiceAndInterface> services;
    private final InetSocketAddress bindAddress;
    private int port;
    private final Configuration conf;

    private Listener listener = null;
    private Responder responder = null;
    private RpcScheduler scheduler = null;

    private int maxQueueSize;
    private int readThreads;
    private int maxIdleTime;
    private boolean tcpNoDelay;
    private boolean tcpKeepAlive;

    private volatile boolean started = false;

    private RpcServer(final String name,
                      final List<BlockingServiceAndInterface> services,
                      final InetSocketAddress bindAddress,
                      Configuration conf,
                      RpcScheduler scheduler) throws IOException {
        this.services = services;
        this.bindAddress = bindAddress;
        this.conf = conf;

        this.maxQueueSize = 1024 * 1024 * 1024; // max callqueue size
        this.readThreads = 10;  // read threadpool size
        this.maxIdleTime = 2 * 1000;    // connection maxidletime

        this.tcpNoDelay = true;
        this.tcpKeepAlive = true;

        this.listener = new Listener(name);
        this.port = listener.getAddress().getPort();
        this.responder = new Responder();
        this.scheduler = scheduler;

    }

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }

        listener.start();
        responder.start();
        scheduler.start();

        started = true;
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

    private class Responder extends Thread {

        @Override
        public void run() {
            super.run();
        }
    }

    private class Listener extends Thread {
        private ServerSocketChannel acceptChannel = null;
        private Selector selector = null;
        private int backlogLength;  // listen queue size

        private Reader[] readers = null;
        private int currentReader = 0;
        private Random random = new Random();

        private ExecutorService readPool;

        public Listener(final String name) throws IOException {
            super(name);

            backlogLength = 128;
            acceptChannel = ServerSocketChannel.open();
            acceptChannel.configureBlocking(false);

            bind(acceptChannel.socket(), bindAddress, backlogLength);
            port = acceptChannel.socket().getLocalPort();

            selector = Selector.open(); // create a selector;

            readers = new Reader[readThreads];
            readPool = Executors.newFixedThreadPool(readThreads, new ThreadFactoryBuilder()
                    .setNameFormat("RpcServer.reader=%d,bindAddress=" + bindAddress.getHostName() + ",port=" + port)
                    .setDaemon(true).build());

            for (int i = 0; i < readThreads; i++) {
                Reader reader = new Reader();
                readers[i] = reader;
                readPool.execute(reader);
            }

            LOG.info(getName() + " started " + readThreads + " reader(s) listening on port=" + port);

            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);

            this.setName("RpcServer.listener,port=" + port);
            this.setDaemon(true);
        }

        private class Reader implements Runnable {

            @Override
            public void run() {

            }
        }

        @Override
        public void run() {
            super.run();
        }

        private InetSocketAddress getAddress() {
            return (InetSocketAddress) acceptChannel.socket().getLocalSocketAddress();
        }

    }

    private static void bind(ServerSocket socket, InetSocketAddress address, int backlog) throws IOException {
        try {
            socket.bind(address, backlog);
        } catch (BindException e) {
            BindException bindException = new BindException("Problem binding to " + address + " : " + e.getMessage());
            bindException.initCause(e);

            throw bindException;
        } catch (SocketException e) {
            if ("Unresolved address".equals(e.getMessage())) {
                throw new UnknownHostException("Invalid hostname for server: " + address.getHostName());
            }

            throw e;
        }
    }
}
