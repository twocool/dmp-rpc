package com.sndo.dmp;

public class ServerName {

    private final String servername;    // 服务hostname
    private final int port; // 服务端口

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
