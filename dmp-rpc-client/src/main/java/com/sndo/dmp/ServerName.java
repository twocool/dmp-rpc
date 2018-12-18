package com.sndo.dmp;

/**
 * @author yangqi
 * @date 2018/12/18 15:23
 **/
public class ServerName {

    private final String servername;
    private final int port;

    private ServerName(final String servername, final int port) {
        this.servername = servername;
        this.port = port;
    }

    public static ServerName valueOf(final String hostname, final int port) {
        return new ServerName(hostname, port);
    }

    public String getServername() {
        return servername;
    }

    public int getPort() {
        return port;
    }
}
