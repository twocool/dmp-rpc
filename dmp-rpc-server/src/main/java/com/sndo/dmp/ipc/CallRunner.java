package com.sndo.dmp.ipc;

import com.google.protobuf.Message;
import com.sndo.dmp.exceptions.ServerNotRunningYetException;
import com.sndo.dmp.ipc.RpcServer.Call;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;

public class CallRunner {

    private static final Log LOG = LogFactory.getLog(CallRunner.class);

    private Call call;
    private RpcServerInterface rpcServer;

    CallRunner(final RpcServerInterface rpcServer, Call call) {
        this.call = call;
        this.rpcServer = rpcServer;
    }

    private void cleanup() {
        this.call = null;
        this.rpcServer = null;
    }

    public void run() {
        try {

            RpcServer.LOG.info(Thread.currentThread().getName() + " : run " + call.toShortString());

            if (!call.connection.channel.isOpen()) {
                if (RpcServer.LOG.isTraceEnabled()) {
                    RpcServer.LOG.debug(Thread.currentThread().getName() + " : skipped " + call);
                }
                return;
            }

            RpcServer.LOG.info(Thread.currentThread().getName() + " : running " + call.toShortString());

            Throwable errorThrowable = null;
            String error = null;
            Pair<Message, CellScanner> resultPair = null;
            RpcServer.CurCall.set(call);
            try {
                if (!this.rpcServer.isStarted()) {
                    InetSocketAddress address = rpcServer.getListenerAddress();
                    throw new ServerNotRunningYetException("Server " +
                            (address != null ? address : "(channel closed)") + " is not running yet");
                }

                RpcServer.LOG.info(Thread.currentThread().getName() + ": " + call.toShortString());
                resultPair = this.rpcServer.call(call.service, call.md, call.param, call.cellScanner, call.timestamp);
            } catch (Throwable e) {
                RpcServer.LOG.debug(Thread.currentThread().getName() + ": " + call.toShortString(), e);
                errorThrowable = e;
                error = StringUtils.stringifyException(e);
                if (e instanceof Error) {
                    throw (Error) e;
                }
            } finally {
                RpcServer.CurCall.set(null);
            }
            // response
            Message param = resultPair != null ? resultPair.getFirst() : null;
            CellScanner cells = resultPair != null ? resultPair.getSecond() : null;
            call.setResponse(param, cells, errorThrowable, error);
            call.sendResponseIfReady();
        } catch (OutOfMemoryError e) {
            if (this.rpcServer.checkOOME(e)) {
                RpcServer.LOG.info(Thread.currentThread().getName() + ": exiting on OutOfMemoryError");
                return;
            }
        } catch (ClosedChannelException cce) {
            InetSocketAddress address = rpcServer.getListenerAddress();
            RpcServer.LOG.warn(Thread.currentThread().getName() + ": caught a ClosedChannelException, " +
                    "this means that the server " + (address != null ? address : "(channel closed)") +
                    " was processing a request but the client went away. The error message was: " +
                    cce.getMessage());
        } catch (Exception e) {
            RpcServer.LOG.warn(Thread.currentThread().getName() +
                    ": caught: " + StringUtils.stringifyException(e));
        } finally {
            cleanup();
        }
    }

}
