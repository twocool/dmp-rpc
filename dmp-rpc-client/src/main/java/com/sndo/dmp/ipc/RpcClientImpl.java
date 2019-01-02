package com.sndo.dmp.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.sndo.dmp.DoNotRetryIOException;
import com.sndo.dmp.ServerName;
import com.sndo.dmp.client.MetricsConnection;
import com.sndo.dmp.exceptions.ConnectionClosingException;
import com.sndo.dmp.protobuf.ProtobufUtil;
import com.sndo.dmp.util.PoolMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.net.NetUtils;

import javax.net.SocketFactory;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcClientImpl extends AbstractRpcClient {

    private static final Log LOG = LogFactory.getLog(RpcClientImpl.class);
    private final SocketFactory socketFactory;
    private final PoolMap<ConnectionId, Connection> connections;

    private final AtomicInteger callIdCnt = new AtomicInteger();

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final FailedServers failedServers;

    public RpcClientImpl(Configuration conf, SocketFactory factory, SocketAddress localAddr) {
        super(conf, localAddr);

        this.socketFactory = factory;
        this.connections = new PoolMap<ConnectionId, Connection>(getPoolType(conf), getPoolSize(conf));
        this.failedServers = new FailedServers(conf);
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
                                              InetSocketAddress addr, MetricsConnection.CallStats callStats) throws IOException, InterruptedException {
        if (pcrc == null) {
            pcrc = new PayloadCarryingRpcController();
        }
        CellScanner cells = pcrc.cellScanner();

        final Call call = new Call(this.callIdCnt.getAndIncrement(), method, param, cells, returnType, pcrc.getCallTimeout(), callStats);
        final Connection connection = getConnection(call, addr);

        final CallFuture callFuture;
        if (connection.callSender != null) {
            // TODO
        } else {
            callFuture = null;
            connection.writeRequest(call, pcrc.getPriority());
        }

        while (!call.done) {
            if (call.checkAndSetTimeout()) {
                // TODO
                break;
            }
            if (connection.shouldCloseConnection.get()) {
                throw new ConnectionClosingException("Call id=" + call.id + " on server " + addr +
                        " aborted: connection is closing");
            }
            try {
                synchronized (call) {
                    if (call.done) {
                        break;
                    }
                    call.wait(Math.min(call.remainingTime(), 1000) + 1);
                }
            } catch (InterruptedException e) {
                call.setException(new InterruptedIOException());
                throw e;
            }
        }

        return null;
    }


    private Connection getConnection(Call call, InetSocketAddress addr) throws IOException {
        if (!running.get()) {
            throw new StoppedRpcClientException();
        }
        Connection connection;
        ConnectionId connectionId = new ConnectionId(call.method.getName(), addr);
        synchronized (connections) {
            connection = connections.get(connectionId);
            if (connection == null) {
                connection = createConnection(connectionId, this.codec, this.compressor);
                connections.put(connectionId, connection);
            }
        }

        return connection;
    }

    private Connection createConnection(ConnectionId connectionId, Codec codec, CompressionCodec compressor) throws UnknownHostException {
        return new Connection(connectionId, codec, compressor);
    }

    private static class CallFuture {
        final Call call;
        final int priority;

        final static CallFuture DEATH_PILL = new CallFuture(null, -1);

        public CallFuture(Call call, int priority) {
            this.call = call;
            this.priority = priority;
        }
    }

    // TODO
    private class Connection extends Thread {
        private ConnectionHeader header;
        private ConnectionId remoteId;
        private Socket socket = null;
        private DataInputStream in;
        private DataOutputStream out;
        private InetSocketAddress server;

        private Object outLock = new Object();  // out write lock

        private final Codec codec;
        private final CompressionCodec compressor;

        private final ConcurrentSkipListMap<Integer, Call> calls = new ConcurrentSkipListMap<Integer, Call>();

        private final AtomicBoolean shouldCloseConnection = new AtomicBoolean();

        private final CallSender callSender;

        // TODO
        private class CallSender extends Thread implements Closeable {

            public CallSender(String name, Configuration conf) {

            }

            @Override
            public void close() {

            }
        }

        public Connection(ConnectionId remoteId, final Codec codec, final CompressionCodec compressor) throws UnknownHostException {
            if (remoteId.getAddress().isUnresolved()) {
                throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
            }

            this.server = remoteId.getAddress();
            this.codec = codec;
            this.compressor = compressor;

            this.remoteId = remoteId;

            ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
            builder.setServiceName(remoteId.getServiceName());
            if (this.codec != null) {
                builder.setCellBlockCodecClass(this.codec.getClass().getCanonicalName());
            }
            if (this.compressor != null) {
                builder.setCellBlockCompressorClass(this.compressor.getClass().getCanonicalName());
            }
            builder.setVersionInfo(ProtobufUtil.getVersionInfo());
            this.header = builder.build();

            this.setName("IPC Client (" + socketFactory.hashCode() + ") connection to " + remoteId.getAddress().toString());
            this.setDaemon(true);

            if (conf.getBoolean(SPECIFIC_WRITE_THREAD, false)) {
                callSender = new CallSender(getName(), conf);
                callSender.start();
            } else {
                callSender = null;
            }
        }

        private void writeRequest(Call call, int priority) throws IOException {
            RequestHeader.Builder builder = RequestHeader.newBuilder();
            builder.setCallId(call.id);
            builder.setMethodName(call.method.getName());
            builder.setRequestParam(call.param != null);
            ByteBuffer cellBlock = ipcUtil.buildCellBlock(this.codec, this.compressor, call.cells);
            if (cellBlock != null) {
                CellBlockMeta.Builder cellBlockMetaBuilder = CellBlockMeta.newBuilder();
                cellBlockMetaBuilder.setLength(cellBlock.limit());
                builder.setCellBlockMeta(cellBlockMetaBuilder.build());
            }

            if (priority != 0) {
                builder.setPriority(priority);
            }

            RequestHeader header = builder.build();

            setupIOstreams();
            checkIsOpen();

            IOException writeException = null;
            synchronized (this.outLock) {
                if (Thread.interrupted()) {
                    throw new InterruptedIOException();
                }

                calls.put(call.id, call);
                checkIsOpen();

                try {
                    call.callStats.setRequestSizeBytes(IPCUtil.write(this.out, header, call.param,  cellBlock));
                } catch (IOException ie) {
                    shouldCloseConnection.set(true);
                    writeException = ie;
                    interrupt();
                }
            }

            if (writeException != null) {
                markClose(writeException);
                close();
            }

            doNotify();

            if (writeException != null) {
                throw writeException;
            }
        }

        private synchronized void doNotify() {
            notifyAll();
        }

        private void checkIsOpen() throws IOException {
            if (shouldCloseConnection.get()) {
                throw new ConnectionClosingException(getName() + " is closing");
            }
        }

        private synchronized void setupIOstreams() throws IOException {
            if (socket != null) {
                return;
            }

            if (shouldCloseConnection.get()) {
                throw new ConnectionClosingException("This connection is closing");
            }

            if (failedServers.isFailedServer(remoteId.getAddress())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Not trying to connect to " + server +
                            " this server is in the failed servers list.");
                }
                IOException e = new FailedServerException(
                        "This server is in the failed servers list:" + server);
                markClose(e);
                close();
                throw e;
            }

            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("connecting to " + server);
                }

                setupConnection();
                InputStream inStream = NetUtils.getInputStream(this.socket);
                OutputStream outStream = NetUtils.getOutputStream(this.socket, writeTimeout);

                this.in = new DataInputStream(new BufferedInputStream(inStream));
                synchronized (this.outLock) {
                    this.out = new DataOutputStream(new BufferedOutputStream(outStream));
                }
                writeConnectionHeader();

                // 开启Connection线程
                start();
                return;
            } catch (Throwable t) {
                IOException e = ExceptionUtil.asInterrupt(t);
                if (e == null) {
                    failedServers.addToFailedServers(remoteId.getAddress());
                    if (t instanceof LinkageError) {
                        e = new DoNotRetryIOException(t);
                    } else if (t instanceof  IOException) {
                        e = (IOException) t;
                    } else {
                        e = new IOException("Cloud not set up IO streams to " + server, t);
                    }
                }
                markClose(e);
                close();
                throw e;
            }
        }

        private synchronized void writeConnectionHeader() throws IOException {
            synchronized (this.outLock) {
                this.out.writeInt(this.header.getSerializedSize());
                this.header.writeTo(this.out);
                this.out.flush();
            }
        }

        private synchronized void setupConnection() throws IOException {
            short ioFailures = 0;
            short timeoutFailures = 0;
            while (true) {
                try {
                    this.socket = socketFactory.createSocket();
                    this.socket.setTcpNoDelay(tcpNoDelay);
                    this.socket.setKeepAlive(tcpKeepAlive);
                    if (localAddr != null) {
                        this.socket.bind(localAddr);
                    }
                    NetUtils.connect(this.socket, remoteId.getAddress(), connectTimeout);
                    this.socket.setSoTimeout(readTimeout);

                    return;
                } catch (SocketTimeoutException toe) {
                    handleConnectionFailure(timeoutFailures++, maxRetries, toe);
                } catch (IOException ie) {
                    handleConnectionFailure(ioFailures++, maxRetries, ie);
                }
            }
        }

        private void handleConnectionFailure(int curRetries, int maxRetries, IOException ioe)
                throws IOException {
            closeConnection();

            if (curRetries  >= maxRetries || ExceptionUtil.isInterrupt(ioe)) {
                throw ioe;
            }

            try {
                Thread.sleep(failureSleep);
            } catch (InterruptedException e) {
                ExceptionUtil.rethrowIfInterrupt(e);
            }

            LOG.info("Retring connect to server: " + remoteId.getAddress() +
                    " after sleeping " + failureSleep + "ms. Already tried " +
                    curRetries + " time(s).");
        }

        private synchronized void closeConnection() {
            if (socket == null) {
                return;
            }

            try {
                if (socket.getInputStream() != null) {
                    socket.getInputStream().close();
                }
            } catch (IOException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("ignored", e);
                }
            }

            try {
                if (socket.getOutputStream() != null) {
                    socket.getOutputStream().close();
                }
            } catch (IOException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("ignored", e);
                }
            }

            try {
                if (socket.getChannel() != null) {
                    socket.getChannel().close();
                }
            } catch (IOException e) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("ignored", e);
                }
            }

            try {
                socket.close();
            } catch (IOException e) {
                LOG.warn("Not able to close a socket", e);
            }

            socket = null;
        }

        // 关闭链接
        private synchronized void close() {
            if (!shouldCloseConnection.get()) {
                LOG.info(getName() + ": the connection is not in the closed state!");
                return;
            }

            synchronized (connections) {
                connections.removeValue(remoteId, this);
            }

            synchronized (this.outLock) {
                if (this.out != null) {
                    IOUtils.closeStream(this.out);
                    this.out = null;
                }
            }

            IOUtils.closeStream(in);
            this.in = null;
            if (this.socket != null) {
                try {
                    this.socket.close();
                    this.socket = null;
                } catch (IOException e) {
                    LOG.error("Error while closing socket", e);
                }
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace(getName() + ": closing ipc connection to " + server);
            }

            cleanupCalls(true);

            if (LOG.isTraceEnabled()) {
                LOG.trace(getName() + ": ipc connection to " + server + " closed");
            }

        }

        private synchronized void cleanupCalls(boolean allCalls) {
            Iterator<Map.Entry<Integer, Call>> iterator = calls.entrySet().iterator();
            while (iterator.hasNext()) {
                Call c = iterator.next().getValue();
                if (c.done) {
                    iterator.remove();
                } else if (allCalls) {
                    long waitTime = EnvironmentEdgeManager.currentTime() - c.getStartTime();
                    IOException e = new IOException("Connection to " + getRemoteAddress()
                    + " is closing. Call id=" + c.id + ", waitTime=" + waitTime);
                    c.setException(e);
                    iterator.remove();
                } else if (c.checkAndSetTimeout()) {
                    iterator.remove();
                } else {
                    // We expect the call to be ordered by timeout.
                    break;
                }
            }
        }

        public InetSocketAddress getRemoteAddress() {
            return remoteId.getAddress();
        }

        private synchronized boolean markClose(IOException e) {
            if (e == null) {
                throw new NullPointerException();
            }

            boolean status = shouldCloseConnection.compareAndSet(false, true);
            if (status) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(getName() + ": marking at should close, reason: " + e.getMessage());
                }
                if (callSender != null) {
                    callSender.close();
                }
                notifyAll();
            }

            return status;
        }

        @Override
        public void run() {
        }

    }

}
