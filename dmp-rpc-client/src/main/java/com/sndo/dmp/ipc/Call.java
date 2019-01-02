package com.sndo.dmp.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.sndo.dmp.client.MetricsConnection;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;

public class Call {

    final int id;   // call id
    final Message param;
    CellScanner cells;
    Message response;
    Message responseDefaultType;
    IOException error;
    volatile boolean done;
    final Descriptors.MethodDescriptor method;
    final int timeout;  // timeout in millisecond for this call; 0 means infinite.
    final MetricsConnection.CallStats callStats;

    Call(int id, final Descriptors.MethodDescriptor method, Message param, final CellScanner cells,
         final Message responseDefaultType, int timeout, MetricsConnection.CallStats callStats) {
        this.id = id;
        this.method = method;
        this.param = param;
        this.cells = cells;
        this.responseDefaultType = responseDefaultType;
        this.timeout = timeout;
        this.callStats = callStats;
    }

    public boolean checkAndSetTimeout() {
        if (timeout == 0) {
            return false;
        }

        long waitTime = EnvironmentEdgeManager.currentTime() - getStartTime();
        if (waitTime > timeout) {
            IOException ie = new CallTimeoutException("Call id=" + id +
            ", waitTime=" + waitTime + ", operateTimeout=" + timeout + " expired.");
            setException(ie);
            return true;
        } else {
            return false;
        }
    }

    public int remainingTime() {
        if (timeout == 0) {
            return Integer.MAX_VALUE;
        }

        int remaining = timeout - (int)(EnvironmentEdgeManager.currentTime() - getStartTime());
        return remaining > 0 ? remaining : 0;

    }

    public synchronized void callComplete() {
        this.done = true;
        notify();
    }

    public void setException(IOException error) {
        this.error = error;
        callComplete();
    }

    public long getStartTime() {
        return this.callStats.getStartTime();
    }

}
