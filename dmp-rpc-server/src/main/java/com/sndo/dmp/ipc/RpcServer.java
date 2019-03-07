package com.sndo.dmp.ipc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.sndo.dmp.exceptions.DoNotRetryIOException;
import com.sndo.dmp.exceptions.UnsupportedCellCodecException;
import com.sndo.dmp.exceptions.UnsupportedCompressionCodecException;
import com.sndo.dmp.protobuf.ProtobufUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RpcServer implements RpcServerInterface {

    public static final Log LOG = LogFactory.getLog(RpcServer.class);

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
    protected final long purgeTimeout;    // in milliseconds

    private volatile boolean started = false;
    private volatile boolean running = true;

    private int socketSendBufferSize;

    private int numConnections = 0;
    private final List<Connection> connectionList = Collections.synchronizedList(new LinkedList<Connection>());

    private static int NIO_BUFFER_LIMIT = 64 * 1024; // should not be more than 64KB.

    private IPCUtil ipcUtil;

    protected static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

    /**
     * How many calls are allowed in the queue.
     */
    static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

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
        this.purgeTimeout = 2 * 60000;

        this.tcpNoDelay = true;
        this.tcpKeepAlive = true;

        this.listener = new Listener(name);
        this.port = listener.getAddress().getPort();
        this.responder = new Responder();
        this.scheduler = scheduler;

        this.socketSendBufferSize = 0;

        this.ipcUtil = new IPCUtil(conf);
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
        return this.started;
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
        if (listener == null) {
            return null;
        }
        return listener.getAddress();
    }

    @Override
    public RpcScheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md, Message param,
                                           CellScanner cellScanner, long receiveTime)
            throws IOException, ServiceException {
        try {
            long startTime = System.currentTimeMillis();
            PayloadCarryingRpcController controller = new PayloadCarryingRpcController(cellScanner);
            Message result = service.callBlockingMethod(md, controller, param);
            long endTime = System.currentTimeMillis();
            int processingTime = (int) (endTime - startTime);
            int qTime = (int) (startTime - receiveTime);
            int totalTime = (int) (endTime - receiveTime);
            if (LOG.isTraceEnabled()) {
                LOG.trace(CurCall.get().toString() +
                        ", response " + TextFormat.shortDebugString(result) +
                        " queueTime: " + qTime +
                        " processingTime: " + processingTime +
                        " totalTime: " + totalTime);
            }

            return new Pair<Message, CellScanner>(result, controller.cellScanner());
        } catch (Throwable e) {
            if (e instanceof ServiceException) {
                e = e.getCause();
            }

            if (e instanceof ServiceException) {
                throw new DoNotRetryIOException(e);
            }

            if (e instanceof IOException) {
                throw (IOException) e;
            }

            LOG.error("Unexcepted throwable object ", e);
            throw new IOException(e.getMessage(), e);
        }
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

        public BlockingService getBlockingService() {
            return this.service;
        }
    }

    static BlockingService getService(final List<BlockingServiceAndInterface> services, final String serviceName) {
        BlockingServiceAndInterface bsasi = getServiceAndInterface(services, serviceName);
        return bsasi == null ? null : bsasi.getBlockingService();
    }

    static BlockingServiceAndInterface getServiceAndInterface(final List<BlockingServiceAndInterface> services,
                                                              final String serviceName) {
        for (BlockingServiceAndInterface bs : services) {
            String blockingServiceName = bs.getBlockingService().getDescriptorForType().getName();
            LOG.info("blockingServiceName: " + blockingServiceName + ", serviceName: " + serviceName);
            if (blockingServiceName.equals(serviceName)) {
                return bs;
            }
        }
        return null;
    }

    public class Connection {
        protected SocketChannel channel;
        private long lastContract;

        private Socket socket;
        private InetAddress addr;
        private int remotePort;
        private String hostAddress;

        private ByteBuffer data;
        private ByteBuffer dataLengthBuffer;

        private Codec codec;
        private CompressionCodec compressionCodec;

        private boolean connectionHeaderRead = false;
        private final Lock responseWriteLock = new ReentrantLock();
        ConnectionHeader connectionHeader;
        BlockingService service;

        private final ConcurrentLinkedDeque<Call> responseQueue = new ConcurrentLinkedDeque<Call>();

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
            if (count < 0 || dataLengthBuffer.remaining() > 0) {
                return count;
            }

            if (data == null) {
                dataLengthBuffer.flip();
                int dataLength = dataLengthBuffer.getInt();
                if (dataLength < 0) {
                    throw new IllegalArgumentException("Unexcepted data length " + dataLength + "!! from " + getHostAddress());
                }
                data = ByteBuffer.allocate(dataLength);
            }

            count = channelRead(channel, data);
            if (count >= 0 && data.remaining() == 0) {
                process();
            }

            return count;
        }

        private void process() throws IOException, InterruptedException {
            data.flip();
            try {
                processOneRpc(data.array());
            } finally {
                dataLengthBuffer.clear(); // Clean for the next call
                data = null;    // For the GC
            }
        }

        private void processOneRpc(byte[] buf) throws IOException, InterruptedException {
            if (connectionHeaderRead) {
                processRequest(buf);
            } else {
                processConnectionHeader(buf);
                this.connectionHeaderRead = true;
            }
        }

        private void processConnectionHeader(byte[] buf) throws IOException {
            this.connectionHeader = ConnectionHeader.parseFrom(buf);
            String serviceName = connectionHeader.getServiceName();
            LOG.info("service: " + serviceName);
            if (serviceName == null) {
                throw new UnknownServiceException(serviceName);
            }
            this.service = getService(services, serviceName);

            setupCellBlockCodecs(connectionHeader);
        }

        private void setupCellBlockCodecs(final ConnectionHeader header) throws UnsupportedCellCodecException, UnsupportedCompressionCodecException {
            if (!header.hasCellBlockCodecClass()) {
                return;
            }

            String className = header.getCellBlockCodecClass();
            if (className == null || className.length() == 0) {
                return;
            }

            try {
                this.codec = (Codec) Class.forName(className).newInstance();
            } catch (Exception e) {
                throw new UnsupportedCellCodecException(className, e);
            }

            if (!header.hasCellBlockCompressorClass()) {
                return;
            }

            className = header.getCellBlockCompressorClass();
            if (className == null || className.length() == 0) {
                return;
            }

            try {
                this.compressionCodec = (CompressionCodec) Class.forName(className).newInstance();
            } catch (Exception e) {
                throw new UnsupportedCompressionCodecException();
            }

        }

        private void processRequest(byte[] buf) throws IOException, InterruptedException {
            long totalRequestSize = buf.length;
            int offset = 0;
            CodedInputStream cis = CodedInputStream.newInstance(buf, offset, buf.length);
            int headerSize = cis.readRawVarint32();
            offset = cis.getTotalBytesRead();
            Message.Builder builder = RequestHeader.newBuilder();
            ProtobufUtil.mergeFrom(builder, buf, offset, headerSize);
            RequestHeader header = (RequestHeader) builder.build();
            offset += headerSize;
            int id = header.getCallId();
            if (LOG.isTraceEnabled()) {
                LOG.trace("RequestHeader " + TextFormat.shortDebugString(header) +
                        " totalRequestSize: " + totalRequestSize + " bytes");
            }

            MethodDescriptor md = null;
            Message param = null;
            CellScanner cellScanner = null;
            try {
                if (header.hasRequestParam() && header.getRequestParam()) {
                    LOG.info("header.hasRequestParam() && header.getRequestParam()");
                    md = this.service.getDescriptorForType().findMethodByName(header.getMethodName());
                    if (md == null) {
                        throw new UnsupportedOperationException(header.getMethodName());
                    }
                    LOG.info("MethodName: " + md.getName());
                    builder = this.service.getRequestPrototype(md).newBuilderForType();
                    cis = CodedInputStream.newInstance(buf, offset, buf.length);
                    int paramSize = cis.readRawVarint32();
                    offset += cis.getTotalBytesRead();
                    if (builder != null) {
                        ProtobufUtil.mergeFrom(builder, buf, offset, paramSize);
                        param = builder.build();
                    }

                    offset += paramSize;
                }
                if (header.hasCellBlockMeta()) {
                    LOG.info("header.hasCellBlockMeta()");
                    cellScanner = ipcUtil.createCellScanner(this.codec, this.compressionCodec, buf, offset, buf.length);
                }
            } catch (Throwable t) {
                t.printStackTrace();
                // TODO 异常处理
            }

            Call call = new Call(id, this.service, md, header, param, cellScanner, this, responder,
                    totalRequestSize, this.addr);
            LOG.info("RequestCall: " + call.toShortString());
            scheduler.dispatch(new CallRunner(RpcServer.this, call));

        }

        private int read4bytes() throws IOException {
            if (dataLengthBuffer.remaining() > 0) {
                return channelRead(channel, dataLengthBuffer);
            } else {
                return 0;
            }
        }

        // TODO
        public boolean timeOut() {
            return false;
        }

        public void close() {
            data = null;
            if (!channel.isOpen())
                return;
            try {
                socket.shutdownOutput();
            } catch (Exception ignored) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(ignored);
                }
            }
            if (channel.isOpen()) {
                try {
                    channel.close();
                } catch (Exception ignored) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(ignored);
                    }
                }
            }
            try {
                socket.close();
            } catch (Exception ignored) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(ignored);
                }
            }
        }

        @Override
        public String toString() {
            return getHostAddress() + ":" + remotePort;
        }
    }

    class Call implements RpcCallContext {
        protected int id;   // 客户端id

        protected BlockingService service;
        protected MethodDescriptor md;
        protected RequestHeader header;
        protected Message param;
        protected CellScanner cellScanner;

        protected Connection connection;
        protected long timestamp;

        private InetAddress remoteAddress;

        protected BufferChain response;
        protected Responder responder;

        protected long size;    // size of current call
        protected boolean isError;

        private ByteBuffer cellBlock = null;

        private long responseCellSize = 0;
        private long responseBlockSize = 0;
//        private boolean retryImmediatelySupported;

        Call(int id, final BlockingService service, final MethodDescriptor md, RequestHeader header,
             Message param, CellScanner cellScanner, Connection connection, Responder responder, long size,
             final InetAddress remoteAddress) {
            this.id = id;
            this.service = service;
            this.md = md;
            this.header = header;
            this.param = param;
            this.cellScanner = cellScanner;
            this.connection = connection;
            this.responder = responder;
            this.size = size;
            this.remoteAddress = remoteAddress;
        }

        @Override
        public InetAddress getRemoteAddress() {
            return this.remoteAddress;
        }

        String toShortString() {
            String serviceName = this.connection.service != null ?
                    this.connection.service.getDescriptorForType().getName() : "null";

            return "callId: " + this.id + " service: " + serviceName +
                    " methodName: " + ((this.md != null) ? this.md.getName() : "n/a") +
                    " size: " + StringUtils.TraditionalBinaryPrefix.long2String(this.size, "", 1) +
                    " connection: " + connection.toString();
        }

        protected synchronized void setResponse(Object m, final CellScanner cells, Throwable t, String errorMsg) {
            if (this.isError) {
                return;
            }

            if (t != null) {
                this.isError = true;
            }

            BufferChain bc = null;
            try {
                ResponseHeader.Builder headerBuilder = ResponseHeader.newBuilder();
                Message result = (Message) m;
                headerBuilder.setCallId(this.id);

                if (t != null) {
                    // TODO nothing
                }
                this.cellBlock = ipcUtil.buildCellBlock(this.connection.codec,
                        this.connection.compressionCodec, cells);

                if (this.cellBlock != null) {
                    CellBlockMeta.Builder cellBlockBuilder = CellBlockMeta.newBuilder();
                    cellBlockBuilder.setLength(this.cellBlock.limit());
                    headerBuilder.setCellBlockMeta(cellBlockBuilder.build());
                }
                Message header = headerBuilder.build();
                ByteBuffer bbHeader = IPCUtil.getDelimitedMessageAsByteBuffer(header);
                ByteBuffer bbResult = IPCUtil.getDelimitedMessageAsByteBuffer(result);
                int totalSize = bbHeader.capacity() + (bbResult == null ? 0 : bbResult.limit()) +
                        (this.cellBlock == null ? 0 : this.cellBlock.limit());
                ByteBuffer bbTotalSize = ByteBuffer.wrap(Bytes.toBytes(totalSize));
                bc = new BufferChain(bbTotalSize, bbHeader, bbResult, this.cellBlock);
            } catch (IOException e) {
                LOG.warn("Exception while creating response " + e);
                e.printStackTrace();
            }
            this.response = bc;
        }

        public synchronized void sendResponseIfReady() throws IOException {
            this.responder.doRespond(this);
        }

        public void done() {
            // nothing
        }

    }

    // TODO
    private class Responder extends Thread {
        private final Selector writeSelector;
        private final Set<Connection> writingCons =
                Collections.newSetFromMap(new ConcurrentHashMap<Connection, Boolean>());

        Responder() throws IOException {
            this.setName("RpcServer.responder");
            this.setDaemon(true);
            writeSelector = Selector.open();
        }

        @Override
        public void run() {
            LOG.info(getName() + ": starting");
            try {
                doRunLoop();
            } finally {
                LOG.info(getName() + ": stopping");
                try {
                    writeSelector.close();
                } catch (IOException e) {
                    LOG.error(getName() + ": cloudn't close write slector", e);
                    e.printStackTrace();
                }
            }
        }

        private void doRunLoop() {
            long lastPurgeTime = 0;
            while (running) {
                try {
                    registerWrites();
                    int keyCnt = writeSelector.select(purgeTimeout);
                    if (keyCnt == 0) {
                        continue;
                    }

                    Set<SelectionKey> keys = writeSelector.selectedKeys();
                    Iterator<SelectionKey> iter = keys.iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        iter.remove();
                        try {
                            if (key.isValid() && key.isWritable()) {
                                doAsyncWrite(key);
                            }
                        } catch (IOException e) {
                            LOG.warn(getName() + " : asyncWrite " + e);
                            e.printStackTrace();
                        }
                    }

                    lastPurgeTime = purge(lastPurgeTime);
                } catch (OutOfMemoryError e) {
                    if (checkOOME(e)) {
                        LOG.info(getName() + ": exiting on OutOfMemoryError");
                        return;
                    }
                } catch (IOException e) {
                    LOG.warn(getName() + ": exception in Responder " +
                            StringUtils.stringifyException(e), e);
                    e.printStackTrace();
                }
            }
        }

        private long purge(long lastPurgeTime) {
            long now = System.currentTimeMillis();
            if (now < lastPurgeTime + purgeTimeout) {
                return lastPurgeTime;
            }

            ArrayList<Connection> conWithOldCalls = new ArrayList<Connection>();

            synchronized (writeSelector.keys()) {
                for (SelectionKey key : writeSelector.keys()) {
                    Connection connection = (Connection) key.attachment();
                    if (connection == null) {
                        throw new IllegalStateException("Coding error: SelectionKey key without attachment.");
                    }
                    Call call = connection.responseQueue.peekFirst();
                    if (call != null && now > call.timestamp + purgeTimeout) {
                        conWithOldCalls.add(call.connection);
                    }
                }
            }

            for (Connection connection : conWithOldCalls) {
                closeConnection(connection);
            }

            return now;
        }

        private void registerWrites() {
            Iterator<Connection> it = writingCons.iterator();
            while (it.hasNext()) {
                Connection c = it.next();
                it.remove();
                SelectionKey selectionKey = c.channel.keyFor(writeSelector);
                try {
                    if (selectionKey == null) {
                        try {
                            c.channel.register(writeSelector, SelectionKey.OP_WRITE, c);
                        } catch (ClosedChannelException e) {
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("ignored", e);
                            }
                            e.printStackTrace();
                        }
                    } else {
                        // ignore: the client went away.
                        selectionKey.interestOps(SelectionKey.OP_WRITE);
                    }
                } catch (CancelledKeyException e) {
                    // ignore: the client went away.
                    if (LOG.isTraceEnabled()) LOG.trace("ignored", e);
                }
            }
        }

        private void doAsyncWrite(SelectionKey key) throws IOException {
            Connection connection = (Connection) key.attachment();
            if (connection == null) {
                throw new IOException("doAsyncWrite: no connection");
            }
            if (key.channel() != connection.channel) {
                throw new IOException("doAsyncWrite: bad channel");
            }

            if (processAllResponses(connection)) {
                try {
                    key.interestOps(0);
                } catch (CancelledKeyException e) {
                    LOG.warn("Exception while changing ops: " + e);
                }
            }
        }

        private boolean processAllResponses(final Connection connection) throws IOException {
            connection.responseWriteLock.lock();
            try {
                for (int i = 0; i < 20; i++) {
                    Call call = connection.responseQueue.pollFirst();
                    if (call == null) {
                        return true;
                    }
                    if (!processResponse(call)) {
                        connection.responseQueue.addFirst(call);
                        return false;
                    }
                }
            } finally {
                connection.responseWriteLock.unlock();
            }

            return connection.responseQueue.isEmpty();
        }

        private boolean processResponse(final Call call) throws IOException {
            boolean error = true;
            try {
                long numBytes = channelWrite(call.connection.channel, call.response);
                if (numBytes < 0) {
                    throw new HBaseIOException("Error writing on socket for the call: " + call.toShortString());
                }
                error = false;
            } finally {
                if (error) {
                    LOG.debug(getName() + call.toShortString() + ": output error -- closing");
                    closeConnection(call.connection);
                }
            }

            if (!call.response.hasRemaining()) {
                call.done();
                return true;
            } else {
                return false;
            }
        }

        public void registerForWrite(Connection connection) {
            if (writingCons.add(connection)) {
                writeSelector.wakeup();
            }
        }

        void doRespond(Call call) throws IOException {
            boolean added = false;

            try {
                if (call.connection.responseQueue.isEmpty() && call.connection.responseWriteLock.tryLock()) {
                    if (call.connection.responseQueue.isEmpty()) {
                        if (processResponse(call)) {
                            return;
                        }
                        call.connection.responseQueue.addFirst(call);
                        added = true;
                    }
                }
            } finally {
                call.connection.responseWriteLock.unlock();
            }

            if (!added) {
                call.connection.responseQueue.addFirst(call);
            }

            call.responder.registerForWrite(call.connection);
            call.timestamp = EnvironmentEdgeManager.currentTime();
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
                e.printStackTrace();
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

    private int channelRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
        return count;
    }

    private static int channelIO(ReadableByteChannel readCh,
                                 WritableByteChannel writeCh,
                                 ByteBuffer buf) throws IOException {

        int originalLimit = buf.limit();
        int initialRemaining = buf.remaining();
        int ret = 0;

        while (buf.remaining() > 0) {
            try {
                int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
                buf.limit(buf.position() + ioSize);

                ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

                if (ret < ioSize) {
                    break;
                }

            } finally {
                buf.limit(originalLimit);
            }
        }

        int nBytes = initialRemaining - buf.remaining();
        return (nBytes > 0) ? nBytes : ret;
    }

    protected long channelWrite(GatheringByteChannel channel, BufferChain bufferChain)
            throws IOException {
        long count = bufferChain.write(channel, NIO_BUFFER_LIMIT);
        return count;
    }

}
