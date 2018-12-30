package com.sndo.dmp.ipc;

import org.apache.hadoop.hbase.HBaseIOException;

/**
 * @author yangqi
 * @date 2018/12/27 11:30
 **/
public class FailedServerException extends HBaseIOException {

    public FailedServerException(String message) {
        super(message);
    }
}
