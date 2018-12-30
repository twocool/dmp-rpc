package com.sndo.dmp;

import org.apache.hadoop.hbase.HBaseIOException;

/**
 * @author yangqi
 * @date 2018/12/26 19:40
 **/
public class DoNotRetryIOException extends HBaseIOException {
    private static final long serialVersionUID = -6950539729309980796L;

    public DoNotRetryIOException() {
        super();
    }

    public DoNotRetryIOException(String message) {
        super(message);
    }

    public DoNotRetryIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public DoNotRetryIOException(Throwable cause) {
        super(cause);
    }

}
