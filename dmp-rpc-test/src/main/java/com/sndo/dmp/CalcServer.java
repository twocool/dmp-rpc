package com.sndo.dmp;

import com.sndo.dmp.ipc.RpcServer;
import com.sndo.dmp.ipc.SimpleRpcScheduler;
import com.sndo.dmp.proto.generated.CalcProto;
import org.apache.hadoop.conf.Configuration;

import java.net.InetSocketAddress;
import java.util.Arrays;

public class CalcServer {

    public static void main(String[] args) throws Exception {
        RpcServer.BlockingServiceAndInterface blockingServiceAndInterface = new RpcServer.BlockingServiceAndInterface(
                CalcProto.CalcService.newReflectiveBlockingService(new CalcServiceImpl()),
                CalcProto.CalcService.BlockingInterface.class);

        InetSocketAddress address = new InetSocketAddress("localhost", 8088);
        Configuration conf = new Configuration();
        SimpleRpcScheduler rpcScheduler = new SimpleRpcScheduler(conf, 1, null);

        RpcServer server = new RpcServer("rpc", Arrays.asList(blockingServiceAndInterface),
                address, conf, rpcScheduler);

        server.start();
        server.join();
    }
}
