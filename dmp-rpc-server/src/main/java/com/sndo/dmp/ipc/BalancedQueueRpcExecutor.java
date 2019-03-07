package com.sndo.dmp.ipc;

import com.sndo.dmp.Abortable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BalancedQueueRpcExecutor extends RpcExecutor {

    private final List<BlockingQueue<CallRunner>> queues;
    private final QueueBalancer balancer;

    public BalancedQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
                                    final int maxQueueLength, final Configuration conf, final Abortable abortable) {
        this(name, handlerCount, numQueues, conf, abortable, LinkedBlockingQueue.class, maxQueueLength);
    }

    public BalancedQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
                                    final Configuration conf, final Abortable abortable,
                                    final Class<? extends BlockingQueue> queueClass, Object... initargs) {
        super(name, Math.max(handlerCount, numQueues), conf, abortable);
        this.queues = new ArrayList<BlockingQueue<CallRunner>>(numQueues);
        this.balancer = getBalancer(numQueues);
        initQueues(numQueues, queueClass, initargs);
    }

    private void initQueues(final int numQueues, final Class<? extends BlockingQueue> queueClass,
                            Object... initargs) {
        for (int i = 0; i < numQueues; i++) {
            this.queues.add((BlockingQueue<CallRunner>) ReflectionUtils.newInstance(queueClass, initargs));
        }
    }

    @Override
    public int getQueueLength() {
        int length = 0;
        for (final BlockingQueue<CallRunner> queue : queues) {
            length += queue.size();
        }

        return length;
    }

    @Override
    public boolean dispatch(CallRunner callTask) throws InterruptedException {
        int queueIndex = balancer.getNextQueue();
        return this.queues.get(queueIndex).offer(callTask);
    }

    @Override
    protected List<BlockingQueue<CallRunner>> getQueues() {
        return this.queues;
    }


}
