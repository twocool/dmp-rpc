package com.sndo.dmp.ipc;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.sndo.dmp.DoNotRetryIOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.BoundedByteBufferPool;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class IPCUtil {

    public static final Log LOG = LogFactory.getLog(IPCUtil.class);
    /**
     * How much we think the decompressor will expand the original compressed content.
     */
    private final int cellBlockDecompressionMultiplier;
    private final int cellBlockBuildingInitialBufferSize;
    private final Configuration conf;

    public IPCUtil(final Configuration conf) {
        super();
        this.conf = conf;
        this.cellBlockDecompressionMultiplier =
                conf.getInt("hbase.ipc.cellblock.decompression.buffersize.multiplier", 3);

        // Guess that 16k is a good size for rpc buffer.  Could go bigger.  See the TODO below in
        // #buildCellBlock.
        this.cellBlockBuildingInitialBufferSize =
                ClassSize.align(conf.getInt("hbase.ipc.cellblock.building.initial.buffersize", 16 * 1024));
    }

    /**
     * Puts CellScanner Cells into a cell block using passed in <code>codec</code> and/or
     * <code>compressor</code>.
     * @param codec
     * @param compressor
     * @param cellScanner
     * @return Null or byte buffer filled with a cellblock filled with passed-in Cells encoded using
     * passed in <code>codec</code> and/or <code>compressor</code>; the returned buffer has been
     * flipped and is ready for reading.  Use limit to find total size.
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public ByteBuffer buildCellBlock(final Codec codec, final CompressionCodec compressor,
                                     final CellScanner cellScanner)
            throws IOException {
        return buildCellBlock(codec, compressor, cellScanner, null);
    }

    public class CellScannerButNoCodecException extends HBaseIOException {};

    /**
     * Puts CellScanner Cells into a cell block using passed in <code>codec</code> and/or
     * <code>compressor</code>.
     * @param codec
     * @param compressor
     * @param cellScanner
     * @param pool Pool of ByteBuffers to make use of. Can be null and then we'll allocate
     * our own ByteBuffer.
     * @return Null or byte buffer filled with a cellblock filled with passed-in Cells encoded using
     * passed in <code>codec</code> and/or <code>compressor</code>; the returned buffer has been
     * flipped and is ready for reading.  Use limit to find total size. If <code>pool</code> was not
     * null, then this returned ByteBuffer came from there and should be returned to the pool when
     * done.
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public ByteBuffer buildCellBlock(final Codec codec, final CompressionCodec compressor,
                                     final CellScanner cellScanner, final BoundedByteBufferPool pool)
            throws IOException {
        if (cellScanner == null) return null;
        if (codec == null) throw new CellScannerButNoCodecException();
        int bufferSize = this.cellBlockBuildingInitialBufferSize;
        ByteBufferOutputStream baos = null;
        if (pool != null) {
            ByteBuffer bb = pool.getBuffer();
            bufferSize = bb.capacity();
            baos = new ByteBufferOutputStream(bb);
        } else {
            // Then we need to make our own to return.
            if (cellScanner instanceof HeapSize) {
                long longSize = ((HeapSize)cellScanner).heapSize();
                // Just make sure we don't have a size bigger than an int.
                if (longSize > Integer.MAX_VALUE) {
                    throw new IOException("Size " + longSize + " > " + Integer.MAX_VALUE);
                }
                bufferSize = ClassSize.align((int)longSize);
            }
            baos = new ByteBufferOutputStream(bufferSize);
        }
        OutputStream os = baos;
        Compressor poolCompressor = null;
        try {
            if (compressor != null) {
                if (compressor instanceof Configurable) ((Configurable)compressor).setConf(this.conf);
                poolCompressor = CodecPool.getCompressor(compressor);
                os = compressor.createOutputStream(os, poolCompressor);
            }
            Codec.Encoder encoder = codec.getEncoder(os);
            int count = 0;
            while (cellScanner.advance()) {
                encoder.write(cellScanner.current());
                count++;
            }
            encoder.flush();
            // If no cells, don't mess around.  Just return null (could be a bunch of existence checking
            // gets or something -- stuff that does not return a cell).
            if (count == 0) return null;
        } catch (BufferOverflowException e) {
            throw new DoNotRetryIOException(e);
        } finally {
            os.close();
            if (poolCompressor != null) CodecPool.returnCompressor(poolCompressor);
        }
        if (LOG.isTraceEnabled()) {
            if (bufferSize < baos.size()) {
                LOG.trace("Buffer grew from initial bufferSize=" + bufferSize + " to " + baos.size() +
                        "; up hbase.ipc.cellblock.building.initial.buffersize?");
            }
        }
        return baos.getByteBuffer();
    }

    public static int write(final OutputStream dos, final Message header, final Message param,
                            final ByteBuffer cellBlock)
            throws IOException {
        // Must calculate total size and write that first so other side can read it all in in one
        // swoop.  This is dictated by how the server is currently written.  Server needs to change
        // if we are to be able to write without the length prefixing.
        int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header, param);
        if (cellBlock != null) totalSize += cellBlock.remaining();
        return write(dos, header, param, cellBlock, totalSize);
    }

    private static int write(final OutputStream dos, final Message header, final Message param,
                             final ByteBuffer cellBlock, final int totalSize)
            throws IOException {
        // I confirmed toBytes does same as DataOutputStream#writeInt.
        dos.write(Bytes.toBytes(totalSize));
        // This allocates a buffer that is the size of the message internally.
        header.writeDelimitedTo(dos);
        if (param != null) param.writeDelimitedTo(dos);
        if (cellBlock != null) dos.write(cellBlock.array(), 0, cellBlock.remaining());
        dos.flush();
        return totalSize;
    }

    public static int getTotalSizeWhenWrittenDelimited(Message ... messages) {
        int totalSize = 0;
        for (Message m: messages) {
            if (m == null) continue;
            totalSize += m.getSerializedSize();
            totalSize += CodedOutputStream.computeRawVarint32Size(m.getSerializedSize());
        }
        Preconditions.checkArgument(totalSize < Integer.MAX_VALUE);
        return totalSize;
    }

}
