package com.sndo.dmp.protobuf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import org.apache.hadoop.hbase.io.LimitInputStream;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.VersionInfo;

import java.io.IOException;
import java.io.InputStream;

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

    /**
     * This version of protobuf's mergeDelimitedFrom avoids the hard-coded 64MB limit for decoding
     * buffers
     * @param builder current message builder
     * @param in Inputsream with delimited protobuf data
     * @throws IOException
     */
    public static void mergeDelimitedFrom(Message.Builder builder, InputStream in)
            throws IOException {
        // This used to be builder.mergeDelimitedFrom(in);
        // but is replaced to allow us to bump the protobuf size limit.
        final int firstByte = in.read();
        if (firstByte != -1) {
            final int size = CodedInputStream.readRawVarint32(firstByte, in);
            final InputStream limitedInput = new LimitInputStream(in, size);
            final CodedInputStream codedInput = CodedInputStream.newInstance(limitedInput);
            codedInput.setSizeLimit(size);
            builder.mergeFrom(codedInput);
            codedInput.checkLastTagWas(0);
        }
    }
}
