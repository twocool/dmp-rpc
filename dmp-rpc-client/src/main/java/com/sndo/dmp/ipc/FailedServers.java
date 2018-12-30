package com.sndo.dmp.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 管理失败的服务
 **/
public class FailedServers {

    private final LinkedList<Pair<Long, String>> failedServers = new LinkedList<>();

    private final int recheckServersTimeout;

    public FailedServers(Configuration conf) {
        this.recheckServersTimeout = conf.getInt(RpcClient.FAILED_SERVER_EXPIRY_KEY, RpcClient.FAILED_SERVER_EXPIRY_DEFAULT);
    }

    public synchronized void addToFailedServers(InetSocketAddress address) {
        final long expiry = EnvironmentEdgeManager.currentTime() + recheckServersTimeout;
        failedServers.add(new Pair<Long, String>(expiry, address.toString()));
    }

    public synchronized boolean isFailedServer(final InetSocketAddress address) {
        if (failedServers.isEmpty()) {
            return false;
        }

        final String lookup = address.toString();
        final long now = EnvironmentEdgeManager.currentTime();

        Iterator<Pair<Long, String>> iterator = failedServers.iterator();
        while (iterator.hasNext()) {
            Pair<Long, String> cur = iterator.next();
            if (cur.getFirst() < now) {
                iterator.remove();
            } else {
                if (lookup.equals(cur.getSecond())) {
                    return true;
                }
            }
        }

        return false;
    }

}
