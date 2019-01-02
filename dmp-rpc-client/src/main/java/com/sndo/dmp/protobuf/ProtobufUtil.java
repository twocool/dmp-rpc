package com.sndo.dmp.protobuf;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.VersionInfo;

public class ProtobufUtil {

    public static HBaseProtos.VersionInfo getVersionInfo() {
        HBaseProtos.VersionInfo.Builder builder = HBaseProtos.VersionInfo.newBuilder();
        builder.setVersion(VersionInfo.getVersion());
        builder.setUrl(VersionInfo.getUrl());
        builder.setRevision(VersionInfo.getRevision());
        builder.setUser(VersionInfo.getUser());
        builder.setDate(VersionInfo.getDate());
        builder.setSrcChecksum(VersionInfo.getSrcChecksum());
        return builder.build();
    }
}
