package com.sndo.dmp.ipc;

import com.sndo.dmp.ByteUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * @author yangqi
 * @date 2018/12/17 19:10
 **/
public class NioClientTest {

    //管道管理器
    private Selector selector;

    public NioClientTest init(String serverIp, int port) throws IOException {
        //获取socket通道
        SocketChannel channel = SocketChannel.open();

        channel.configureBlocking(false);
        //获得通道管理器
        selector = Selector.open();

        //客户端连接服务器，需要调用channel.finishConnect();才能实际完成连接。
        channel.connect(new InetSocketAddress(serverIp, port));
        //为该通道注册SelectionKey.OP_CONNECT事件
        channel.register(selector, SelectionKey.OP_CONNECT);
        return this;
    }

    public void listen() throws IOException {
        System.out.println("客户端启动");
        //轮询访问selector
        while (true) {
            //选择注册过的io操作的事件(第一次为SelectionKey.OP_CONNECT)
            selector.select();
            Iterator<SelectionKey> ite = selector.selectedKeys().iterator();
            while (ite.hasNext()) {
                SelectionKey key = ite.next();
                //删除已选的key，防止重复处理
                ite.remove();
                if (key.isConnectable()) {
                    SocketChannel channel=(SocketChannel)key.channel();

                    //如果正在连接，则完成连接
                    if(channel.isConnectionPending()){
                        channel.finishConnect();
                    }

                    channel.configureBlocking(false);

                    // begin
                    ByteBuffer dataLengthBuffer = ByteBuffer.allocate(4);
                    String message = "hello world!";

                    int dataLength = message.length();
                    dataLengthBuffer.put(ByteUtil.intToByte(dataLength));
                    dataLengthBuffer.flip();
                    channel.write(dataLengthBuffer);

                    ByteBuffer dataBuffer = ByteBuffer.allocate(dataLength);
                    dataBuffer.put(message.getBytes());
                    dataBuffer.flip();
                    channel.write(dataBuffer);
                    // end

                    //连接成功后，注册接收服务器消息的事件
                    channel.register(selector, SelectionKey.OP_READ);
                    System.out.println("客户端连接成功");
                } else if (key.isReadable()) { //有可读数据事件。
                    SocketChannel channel = (SocketChannel) key.channel();

                    ByteBuffer buffer = ByteBuffer.allocate(10);
                    channel.read(buffer);
                    byte[] data = buffer.array();
                    String message = new String(data);

                    System.out.println("recevie message from server:, size:" + buffer.position() + " msg: " + message);
//                    ByteBuffer outbuffer = ByteBuffer.wrap(("client.".concat(msg)).getBytes());
//                    channel.write(outbuffer);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
//        new NioClient().init("127.0.0.1", 9981).listen();
        new NioClientTest().init("localhost", 8083).listen();
    }
}
