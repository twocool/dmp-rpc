package com.sndo.dmp.ipc;

import java.net.InetSocketAddress;

public class ConnectionId {

    final String serviceName;
    final InetSocketAddress address;

    public ConnectionId(String serviceName, InetSocketAddress address) {
        this.serviceName = serviceName;
        this.address = address;
    }

    public String getServiceName() {
        return serviceName;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return "ConnectionId{" +
                "serviceName='" + serviceName + '\'' +
                ", address=" + address +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConnectionId that = (ConnectionId) o;

        if (serviceName != null ? !serviceName.equals(that.serviceName) : that.serviceName != null) return false;
        return address != null ? address.equals(that.address) : that.address == null;
    }

    @Override
    public int hashCode() {
        int result = serviceName != null ? serviceName.hashCode() : 0;
        result = 31 * result + (address != null ? address.hashCode() : 0);
        return result;
    }
}
