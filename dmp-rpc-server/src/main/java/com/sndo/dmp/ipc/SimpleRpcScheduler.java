package com.sndo.dmp.ipc;

import com.sndo.dmp.Abortable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class SimpleRpcScheduler extends RpcScheduler {

    private static final Log LOG = LogFactory.getLog(SimpleRpcScheduler.class);

    private int port;
    private final RpcExecutor callExecutor;
    private Abortable abortable = null;

    public static final String CALL_QUEUE_HANDLER_FACTOR_CONF_KEY = "hbase.ipc.server.callqueue.handler.factor";

    public SimpleRpcScheduler(Configuration conf, int handlerCount, Abortable server) {
        this.abortable = server;

        int maxQueueLength = conf.getInt("hbase.ipc.server.max.callqueue.length",
                handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);

        float callQueuesHandlersFactor = conf.getFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 0);
        int numCallQueues = Math.max(1, (int)Math.round(handlerCount * callQueuesHandlersFactor));

        this.callExecutor = new BalancedQueueRpcExecutor("B.default", handlerCount, numCallQueues,
                maxQueueLength, conf, abortable);
    }

    @Override
    public void init(Context context) {
        this.port = context.getListenerAddress().getPort();
    }

    @Override
    public void start() {
        callExecutor.start(port);
    }

    @Override
    public void stop() {
        callExecutor.stop();
    }

    @Override
    public void dispatch(CallRunner task) throws InterruptedException {
        callExecutor.dispatch(task);
    }
}
