package com.sndo.dmp.ipc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sndo.dmp.Abortable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

public abstract class RpcExecutor {
    private static final Log LOG = LogFactory.getLog(RpcExecutor.class);

    private final List<Thread> handlers;
    private final int handlerCount;
    private final String name;

    private boolean running;
    private Configuration conf = null;
    private Abortable abortable = null;

    public RpcExecutor(final String name, final int handlerCount) {
        this.name = Strings.nullToEmpty(name);
        this.handlerCount = handlerCount;
        this.handlers = new ArrayList<Thread>(handlerCount);
    }

    public RpcExecutor(final String name, final int handlerCount, final Configuration conf,
                       final Abortable abortable) {

        this(name, handlerCount);
        this.conf = conf;
        this.abortable = abortable;
    }

    public void start(final int port) {
        running = true;
        startHandlers(port);
    }

    public void stop() {
        running = false;
        for (Thread handler : handlers) {
            handler.interrupt();
        }
    }

//    public int getActiveHandlerCount() {
//        return activeHandlerCount.get();
//    }

    public abstract int getQueueLength();

    public abstract boolean dispatch(final CallRunner callTask) throws InterruptedException;

    protected abstract List<BlockingQueue<CallRunner>> getQueues();

    protected void startHandlers(final int port) {
        List<BlockingQueue<CallRunner>> callQueues = getQueues();
        startHandlers(null, handlerCount, callQueues, 0, callQueues.size(), port);
    }

    protected void startHandlers(final String nameSuffix, final int numHandlers,
                                 final List<BlockingQueue<CallRunner>> callQueues,
                                 final int qIndex, final int qSize, final int port) {
        final String threadPrefix = name + Strings.nullToEmpty(nameSuffix);
        for (int i = 0; i < numHandlers; i++) {
            final int index = qIndex + (i % qSize);
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    consumerLoop(callQueues.get(index));
                }
            });
            t.setDaemon(true);
            t.setName(threadPrefix + "RpcServer.handler=" + handlers.size() +
                    ",queue=" + index + ",port=" + port);
            t.start();
            LOG.debug(threadPrefix + " Start Handler index=" + handlers.size() + " queue=" + index);
            handlers.add(t);
        }
    }

    protected void consumerLoop(final BlockingQueue<CallRunner> myQueue) {
        boolean interrupted = false;
        try {
            while (running) {
                try {
                    CallRunner task = myQueue.take();
                    try {
                        task.run();
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                    e.printStackTrace();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }


    public static abstract class QueueBalancer {

        public abstract int getNextQueue();

    }

    public static QueueBalancer getBalancer(int queueSize) {
        Preconditions.checkArgument(queueSize > 0, "Queue size is <= 0, must be at least 1");
        if (queueSize == 1) {
            return ONE_QUEUE;
        } else {
            return new RandomQueueBalancer(queueSize);
        }
    }

    private static QueueBalancer ONE_QUEUE = new QueueBalancer() {
        @Override
        public int getNextQueue() {
            return 0;
        }
    };

    private static class RandomQueueBalancer extends QueueBalancer {
        private final int queueSize;

        public RandomQueueBalancer(int queueSize) {
            this.queueSize = queueSize;
        }

        @Override
        public int getNextQueue() {
            return ThreadLocalRandom.current().nextInt(queueSize);
        }
    }

}
