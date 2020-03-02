package me.gaigeshen.fastdfs.client.connection;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * 剩余数据的提供，用于发送持续发送剩余的数据，如果不能一次性发送完毕，可采用此类解决
 *
 * @author gaigeshen
 */
public class RemainingDataSupplier {
  private final boolean closeOnFinish;
  private final InputStream in;

  private RemainingDataSupplier(boolean closeOnFinish, InputStream in) {
    this.closeOnFinish = closeOnFinish;
    this.in = in;
  }

  /**
   * 创建此对象
   *
   * @param closeOnFinish 是否在数据提供完毕之后关闭输入流
   * @param in 数据输入流
   * @return 此对象
   */
  public static RemainingDataSupplier create(boolean closeOnFinish, InputStream in) {
    return new RemainingDataSupplier(closeOnFinish, in);
  }

  /**
   * 创建此对象
   *
   * @param in 数据输入流，默认情况下，会在数据提供完毕之后关闭此输入流
   * @return 此对象
   */
  public static RemainingDataSupplier create(InputStream in) {
    return create(true, in);
  }

  /**
   * 将输入流的数据写出到链接通道
   *
   * @param channel 链接通道
   * @throws IOException 发生异常
   */
  void readData(SocketChannel channel) throws IOException {
    try {
      int len;
      byte[] buffer = new byte[4096];
      while ((len = in.read(buffer)) != -1) {
        channel.write(ByteBuffer.wrap(buffer, 0, len));
      }
    } finally {
      if (closeOnFinish) {
        in.close();
      }
    }
  }
}
