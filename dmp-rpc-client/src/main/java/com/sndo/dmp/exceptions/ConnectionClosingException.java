package com.sndo.dmp.exceptions;

import java.io.IOException;

/**
 * @author yangqi
 * @date 2018/12/26 21:21
 **/
public class ConnectionClosingException extends IOException {
    private static final long serialVersionUID = -8137305982238768143L;

    public ConnectionClosingException(String message) {
        super(message);
    }
}
