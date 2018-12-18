package com.sndo.dmp.ipc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.BlockingService;
import com.sndo.dmp.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RpcServer implements RpcServerInterface, RpcErrorHandler {

    private static final Log LOG = LogFactory.getLog(RpcServer.class);

    private final List<BlockingServiceAndInterface> services;
    private final InetSocketAddress bindAddress;
    private int port;
    private final Configuration conf;

    private Listener listener = null;
    private Responder responder = null;
    private RpcScheduler scheduler = null;

    private int maxQueueSize;
    private int readThreadSize;
    private int maxIdleTime;
    private boolean tcpNoDelay;
    private boolean tcpKeepAlive;
    private int thresholdIdleConnections;
    private int maxConnections2Nuke;

    private volatile boolean started = false;
    private volatile boolean running = true;

    private int socketSendBufferSize;

    private int numConnections = 0;
    private final List<Connection> connectionList = Collections.synchronizedList(new LinkedList<Connection>());

    private static int NIO_BUFFER_LIMIT = 64 * 1024; // should not be more than 64KB.

    public RpcServer(final String name,
                      final List<BlockingServiceAndInterface> services,
                      final InetSocketAddress bindAddress,
                      Configuration conf,
                      RpcScheduler scheduler) throws IOException {
        this.services = services;
        this.bindAddress = bindAddress;
        this.conf = conf;

        this.maxQueueSize = 1024 * 1024 * 1024; // max callqueue size
        this.readThreadSize = 1;  // read threadpool size
        this.maxIdleTime = 2 * 1000;    // connection maxidletime
        this.thresholdIdleConnections = 4000;   // client idlethreshold
        this.maxConnections2Nuke = 10; // client kill max

        this.tcpNoDelay = true;
        this.tcpKeepAlive = true;

        this.listener = new Listener(name);
        this.port = listener.getAddress().getPort();
        this.responder = new Responder();
        this.scheduler = scheduler;

        this.socketSendBufferSize = 0;
    }

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }

        listener.start();
        responder.start();
//        scheduler.start();

        started = true;
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public synchronized void join() throws InterruptedException {
        while (running) {
            wait();
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public void setSocketSendBufSize(int size) {
        this.socketSendBufferSize = size;
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

    @Override
    public boolean checkOOME(Throwable e) {
        boolean stop = false;
        try {
            if ((e instanceof OutOfMemoryError)
                    || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
                    || (e.getMessage() != null && e.getMessage().contains("java.lang.OutOfMemoryError"))) {
                stop = true;
                LOG.fatal("Run out of memory; " + getClass().getSimpleName() + " will abort itself immediately.", e);
            }
        } finally {
            if (stop) {
                Runtime.getRuntime().halt(1);
            }
        }

        return stop;
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

    public class Connection {
        private SocketChannel channel;
        private long lastContract;

        private Socket socket;
        private InetAddress addr;
        private int remotePort;
        private String hostAddress;

        private ByteBuffer data;
        private ByteBuffer dataLengthBuffer;

        public Connection(SocketChannel channel, long lastContract) {
            this.channel = channel;
            this.lastContract = lastContract;

            this.socket = channel.socket();
            this.addr = socket.getInetAddress();
            if (addr == null) {
                this.hostAddress = "*Unknown*";
            } else {
                this.hostAddress = addr.getHostAddress();
            }

            this.remotePort = socket.getPort();

            this.data = null;
            this.dataLengthBuffer = ByteBuffer.allocate(4);

            if (socketSendBufferSize != 0) {
                try {
                    socket.setSendBufferSize(socketSendBufferSize);
                } catch (SocketException e) {
                    LOG.warn("Connection: unable to set socket send buffer size to " + socketSendBufferSize);
                }
            }
        }

        public long getLastContract() {
            return lastContract;
        }

        public void setLastContract(long lastContract) {
            this.lastContract = lastContract;
        }

        public String getHostAddress() {
            return this.hostAddress;
        }

        public int readAndProcess() throws InterruptedException, IOException {
            int count = read4bytes();
            if (count < 0) {
                return count;
            }

            dataLengthBuffer.flip();
            int dataLength = dataLengthBuffer.getInt();
            data = ByteBuffer.allocate(dataLength);

            count = channelRead(channel, data);

            if (count >= 0 || data.remaining() == 0) {
                process();
            }

            return count;
        }

        // TODO
        private void process() {
            try {
                System.out.println(new String(data.array(), "utf-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        private int read4bytes() throws IOException {
            if (dataLengthBuffer.remaining() > 0) {
                return channelRead(channel, dataLengthBuffer);
            } else {
                return 0;
            }
        }

        private int channelRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
            int count = buffer.remaining() <= NIO_BUFFER_LIMIT ? channel.read(buffer) : channelIO(channel, null, buffer);
            return count;
        }

        private int channelIO(ReadableByteChannel readCh, WritableByteChannel writeCh, ByteBuffer buffer) throws IOException {
            int originalLimit = buffer.limit();
            int initialRemaining = buffer.remaining();

            int ret = 0;
            while (buffer.remaining() > 0) {
                try {
                    int ioSize = Math.min(buffer.remaining(), NIO_BUFFER_LIMIT);
                    buffer.limit(buffer.position() + ioSize);

                    ret = readCh != null ? readCh.read(buffer) : writeCh.write(buffer);

                    if (ret < ioSize) {
                        break;
                    }
                } finally {
                    buffer.limit(originalLimit);
                }
            }

            int nBytes = initialRemaining - buffer.remaining();
            return nBytes > 0 ? nBytes : ret;
        }

        // TODO
        public boolean timeOut() {
            return false;
        }

        // TODO
        public void close() {

        }

        @Override
        public String toString() {
            return getHostAddress() + ":" + remotePort;
        }
    }

    private class Responder extends Thread {

        @Override
        public void run() {

        }

    }

    private class Listener extends Thread {
        private ServerSocketChannel acceptChannel = null;
        private Selector selector = null;
        private int backlogLength;  // listen queue size

        private Reader[] readers = null;
        private int currentReader = 0;

        private Random random = new Random();
        private long lastCleanupRuntime = 0L;
        private long cleanupInterval = 10 * 1000L;

        private ExecutorService readPool;

        public Listener(final String name) throws IOException {
            super(name);

            backlogLength = 128;
            acceptChannel = ServerSocketChannel.open();
            acceptChannel.configureBlocking(false);

            bind(acceptChannel.socket(), bindAddress, backlogLength);
            port = acceptChannel.socket().getLocalPort();

            selector = Selector.open(); // create a selector;

            readers = new Reader[readThreadSize];
            readPool = Executors.newFixedThreadPool(readThreadSize, new ThreadFactoryBuilder()
                    .setNameFormat("RpcServer.reader=%d, bindAddress=" + bindAddress.getHostName() + ", port=" + port)
                    .setDaemon(true).build());

            for (int i = 0; i < readThreadSize; i++) {
                Reader reader = new Reader();
                readers[i] = reader;
                readPool.execute(reader);
            }

            LOG.info(getName() + " started " + readThreadSize + " reader(s) listening on port=" + port);

            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);

            this.setName("RpcServer.listener, port=" + port);
            this.setDaemon(true);
        }

        private class Reader implements Runnable {
            private volatile boolean adding = false;
            private final Selector readSelector;

            public Reader() throws IOException {
                this.readSelector = Selector.open();
            }

            @Override
            public void run() {
                try {
                    doRunLoop();
                } finally {
                    try {
                        readSelector.close();
                    } catch (IOException ioe) {
                        LOG.error(getName() + ": error closing read selector in " + getName(), ioe);
                    }
                }
            }

            private synchronized void doRunLoop() {
                while (running) {
                    try {
                        readSelector.select();
                        while (adding) {
                            this.wait(1000);
                        }
                        Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
                        while (iter.hasNext()) {
                            SelectionKey key = iter.next();
                            iter.remove();
                            if (key.isValid()) {
                                if (key.isReadable()) {
                                    doRead(key);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.debug("interrupted while sleeping.");
                    } catch (IOException e) {
                        LOG.info(getName() + ": IOException in Reader", e);
                    }
                }
            }

            public void startAdd() {
                adding = true;
                readSelector.wakeup();
            }

            public void finishAdd() {
                adding = false;
                this.notify();
            }

            public synchronized SelectionKey registerChannel(SocketChannel channel) throws ClosedChannelException {
                return channel.register(readSelector, SelectionKey.OP_READ);
            }

        }

        @Override
        public void run() {
            LOG.info(getName() + ": starting.");
            SelectionKey key = null;
            while (running) {
                try {
                    selector.select();
                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                    while (iterator.hasNext()) {
                        key = iterator.next();
                        iterator.remove();
                        if (key.isValid()) {
                            if (key.isAcceptable()) {
                                doAccept(key);
                            }
                        }
                    }
                } catch (OutOfMemoryError e) {
                    if (checkOOME(e)) {
                        LOG.info(getName() + ": exiting on outOfMemoryError.");
                        closeCurrentConnection(key, e);
                        cleanupConnections(true);
                        return;
                    }
                } catch (Exception e) {
                    closeCurrentConnection(key, e);
                }
                cleanupConnections(false);
            }

            LOG.info(getName() + ": stopping.");

            synchronized (this) {
                try {
                    acceptChannel.close();
                    selector.close();
                } catch (IOException e) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(getName() + ": ignored", e);
                    }
                }

                selector = null;
                acceptChannel = null;

                while (!connectionList.isEmpty()) {
                    closeConnection(connectionList.remove(0));
                }
            }
        }

        private void doRead(SelectionKey key) throws InterruptedException {
            Connection c = (Connection) key.attachment();
            if (c == null) {
                return;
            }
            c.setLastContract(System.currentTimeMillis());

            int count;
            try {
                count = c.readAndProcess();
                if (count > 0) {
                    c.setLastContract(System.currentTimeMillis());
                }
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(getName() + ": Caught exception while reading: " + e.getMessage());
                }
                count = -1;
            }

            if (count < 0) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(getName() + ": disconnecting client " + c.toString() +
                            "because read count=" + count +
                            ". Number of active connection= " + numConnections);
                }
                closeConnection(c);
            }
        }

        private void cleanupConnections(boolean force) {
            if (force || numConnections > thresholdIdleConnections) {
                long currentTime = System.currentTimeMillis();
                if (!force && (currentTime - lastCleanupRuntime) < cleanupInterval) {
                    return;
                }

                int start = 0;
                int end = numConnections - 1;
                if (!force) {
                    start = random.nextInt() % numConnections;
                    end = random.nextInt() % numConnections;

                    int temp;
                    if (start > end) {
                        temp = start;
                        start = end;
                        end = temp;
                    }
                }

                int i = start;
                int numNuked = 0;
                while (i <= end) {
                    Connection c;
                    synchronized (connectionList) {
                        c = connectionList.get(i);
                    }
                    if (c.timeOut()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
                        }

                        closeConnection(c);
                        numNuked++;
                        end--;

                        c = null;

                        if (!force && numNuked == maxConnections2Nuke) {
                            break;
                        }
                    } else {
                        i++;
                    }
                }
                lastCleanupRuntime = System.currentTimeMillis();
            }
        }

        private void closeCurrentConnection(SelectionKey key, Throwable e) {
            if (key != null) {
                Connection c = (Connection) key.attachment();
                if (c != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getName() + ": disconnecting client " + c.getHostAddress() +
                                (e != null ? "on error " + e.getMessage() : ""));
                    }

                    closeConnection(c);
                }
            }
        }

        private void doAccept(SelectionKey key) throws IOException {
            ServerSocketChannel sever = (ServerSocketChannel) key.channel();

            SocketChannel channel;
            while ((channel = sever.accept()) != null) {
                try {
                    channel.configureBlocking(false);
                    channel.socket().setKeepAlive(tcpKeepAlive);
                    channel.socket().setTcpNoDelay(tcpNoDelay);
                } catch (IOException ioe) {
                    channel.close();
                    throw ioe;
                }

                Reader reader = getReader();
                try {
                    reader.startAdd();
                    SelectionKey readKey = reader.registerChannel(channel);
                    Connection c = getConnection(channel, System.currentTimeMillis());
                    readKey.attach(c);

                    synchronized (connectionList) {
                        connectionList.add(numConnections, c);
                        numConnections++;
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(getName() + ": connection from " + c.toString() +
                                "; # active connections: " + numConnections);
                    }
                } finally {
                    reader.finishAdd();
                }

            }
        }

        private Reader getReader() {
            currentReader = (currentReader + 1) % readers.length;
            return readers[currentReader];
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

    private Connection getConnection(SocketChannel channel, long time) {
        return new Connection(channel, time);
    }

    private void closeConnection(Connection c) {
        synchronized (connectionList) {
            if (connectionList.remove(c)) {
                numConnections--;
            }
        }
        c.close();
    }

    private int channelRead(AbstractSelectableChannel channel, ByteBuffer buffer) {

        return -1;
    }

}
