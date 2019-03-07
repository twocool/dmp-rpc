package com.sndo.dmp.exceptions;

/**
 * @author yangqi
 * @date 2019/2/26 16:03
 **/
public class UnsupportedCompressionCodecException extends FatalConnectionException {

    public UnsupportedCompressionCodecException() {
        super();
    }

    public UnsupportedCompressionCodecException(String message) {
        super(message);
    }

    public UnsupportedCompressionCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
