package me.gaigeshen.fastdfs.client.connection;

import java.net.InetSocketAddress;

/**
 * 链接选项
 *
 * @author gaigeshen
 */
public class ConnectionOptions {
  private final InetSocketAddress socketAddress;

  private ConnectionOptions(InetSocketAddress socketAddress) {
    this.socketAddress = socketAddress;
  }

  /**
   * 创建链接选项
   *
   * @param socketAddress 链接地址对象
   * @return 链接选项
   */
  public static ConnectionOptions create(InetSocketAddress socketAddress) {
    return new ConnectionOptions(socketAddress);
  }

  /**
   * 创建链接选项
   *
   * @param host 地址
   * @param port 端口
   * @return 链接选项
   */
  public static ConnectionOptions create(String host, int port) {
    return create(new InetSocketAddress(host, port));
  }

  /**
   * 返回链接地址对象
   *
   * @return 链接地址对象
   */
  public InetSocketAddress getSocketAddress() {
    return socketAddress;
  }
}
