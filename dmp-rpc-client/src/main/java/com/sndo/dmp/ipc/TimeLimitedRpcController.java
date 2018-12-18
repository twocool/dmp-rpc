package com.sndo.dmp.ipc;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

/**
 * @author yangqi
 * @date 2018/12/18 16:24
 **/
public class TimeLimitedRpcController implements RpcController {

    private volatile Integer callTimeout;

    public Integer getCallTimeout() {
        if (callTimeout != null) {
            return callTimeout;
        } else {
            return 0;
        }
    }

    public void setCallTimeout(Integer callTimeout) {
        this.callTimeout = callTimeout;
    }

    public boolean hasCallTimeout() {
        return this.callTimeout != null;
    }

    @Override
    public void reset() {
    }

    @Override
    public boolean failed() {
        return false;
    }

    @Override
    public String errorText() {
        return null;
    }

    @Override
    public void startCancel() {
    }

    @Override
    public void setFailed(String reason) {
    }

    @Override
    public boolean isCanceled() {
        return false;
    }

    @Override
    public void notifyOnCancel(RpcCallback<Object> callback) {
    }
}
