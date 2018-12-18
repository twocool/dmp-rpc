package com.sndo.dmp.ipc;

import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author yangqi
 * @date 2018/12/14 15:06
 **/
public class RpcServerTest {

    @Test
    public void testRpcServer() throws IOException, InterruptedException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 8083);
        RpcServer server = new RpcServer("rpcServer", null, inetSocketAddress, null, null);
        server.start();
        server.join();
    }
}
