package me.gaigeshen.fastdfs.client.connection;

import org.apache.commons.lang3.Validate;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * 链接对象
 *
 * @author gaigeshen
 */
public class Connection implements Closeable {
  private final SocketChannel channel;

  /**
   * 创建连接对象
   *
   * @param socketAddress 连接地址
   * @throws IOException 发生异常，创建的时候，会去连接该地址
   */
  Connection(InetSocketAddress socketAddress) throws IOException {
    this.channel = buildSocketChannel(socketAddress);
  }

  /**
   * 构建套接字通道
   *
   * @param socketAddress 连接地址
   * @return 套接字通道
   * @throws IOException 发生异常
   */
  private SocketChannel buildSocketChannel(InetSocketAddress socketAddress) throws IOException {
    SocketChannel sc = SocketChannel.open();
    if (sc.isOpen()) {
      sc.configureBlocking(true);
      sc.setOption(StandardSocketOptions.SO_RCVBUF, 128 * 1024);
      sc.setOption(StandardSocketOptions.SO_SNDBUF, 128 * 1024);
      sc.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
      sc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
      sc.setOption(StandardSocketOptions.SO_LINGER, 5);
      sc.connect(socketAddress);
    }
    return sc;
  }

  /**
   * 发送数据
   *
   * @param data 数据
   * @throws IOException 发生异常
   */
  public void send(byte[] data) throws IOException {
    Validate.isTrue(isConnected(), "Not connected");
    channel.write(ByteBuffer.wrap(data));
  }

  /**
   * 发送数据然后接收返回的数据
   *
   * @param data 数据
   * @param resultHandler 接收返回的数据
   * @throws IOException 发生异常
   */
  public void sendAndReceived(byte[] data, ResultHandler resultHandler) throws IOException {
    sendAndReceived(data, null, resultHandler);
  }

  /**
   * 发送数据然后接收返回的数据
   *
   * @param data 数据
   * @param supplier 剩余的数据，可能用于上传文件
   * @param resultHandler 接收返回的数据
   * @throws IOException 发生异常
   */
  public void sendAndReceived(byte[] data, RemainingDataSupplier supplier, ResultHandler resultHandler) throws IOException {
    Validate.isTrue(isConnected(), "Not connected");
    channel.write(ByteBuffer.wrap(data));
    if (supplier != null) {
      supplier.readData(channel);
    }
    ByteBuffer result = ByteBuffer.allocate(4096);
    while (channel.read(result) > 0) {
      result.flip();
      byte[] bResult = new byte[result.remaining()];
      result.get(bResult);
      result.clear();
      if (!resultHandler.handle(bResult)) {
        break;
      }
    }
  }

  /**
   * 判断是否已经链接
   *
   * @return 返回是否已经链接
   */
  public boolean isConnected() {
    return channel != null && channel.isConnected();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public String toString() {
    return String.format("Connection[%s]", channel);
  }
}
