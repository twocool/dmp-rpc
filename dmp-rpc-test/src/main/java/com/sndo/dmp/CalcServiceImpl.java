package com.sndo.dmp;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.sndo.dmp.proto.generated.CalcProto;

public class CalcServiceImpl implements CalcProto.CalcService.BlockingInterface {

    @Override
    public CalcProto.CalcResponse add(RpcController controller, CalcProto.CalcRequest request) throws ServiceException {
        int sum = request.getParam1() + request.getParam2();
        CalcProto.CalcResponse.Builder builder = CalcProto.CalcResponse.newBuilder();
        builder.setResult(sum);
        System.out.println("SUM: " + sum);
        return builder.build();
    }

    @Override
    public CalcProto.CalcResponse minus(RpcController controller, CalcProto.CalcRequest request) throws ServiceException {
        int result = request.getParam1() - request.getParam2();

        CalcProto.CalcResponse.Builder builder = CalcProto.CalcResponse.newBuilder();
        builder.setResult(result);

        return builder.build();
    }
}

