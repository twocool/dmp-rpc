package com.sndo.dmp;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.ServiceException;
import com.sndo.dmp.ipc.RpcClient;
import com.sndo.dmp.ipc.RpcClientImpl;
import com.sndo.dmp.proto.generated.CalcProto;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CalcClient {

    public static void main(String[] args) throws ServiceException, IOException {
        RpcClient rpcClient = new RpcClientImpl(new Configuration(), null);
        BlockingRpcChannel blockingRpcChannel =
                rpcClient.createBlockingRpcChannel(ServerName.valueOf("localhost", 8088), 60000);
        CalcProto.CalcService.BlockingInterface stub = CalcProto.CalcService.newBlockingStub(blockingRpcChannel);

        for (int i = 0; i < 600; i++) {
            CalcProto.CalcRequest.Builder builder = CalcProto.CalcRequest.newBuilder();
            builder.setParam1(i);
            builder.setParam2(i);
            CalcProto.CalcRequest request = builder.build();
            CalcProto.CalcResponse response = stub.add(null, request);
            int sum = response.getResult();
            System.out.println(sum);
        }

        rpcClient.close();
    }
}
