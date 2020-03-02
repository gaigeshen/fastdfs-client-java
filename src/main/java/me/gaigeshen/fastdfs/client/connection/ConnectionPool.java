package me.gaigeshen.fastdfs.client.connection;

import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

/**
 * 链接对象池
 *
 * @author gaigeshen
 */
@Slf4j
public class ConnectionPool extends Pool<Connection> {
  /**
   * 创建链接对象池
   *
   * @param poolConfig 对象池的配置
   * @param socketAddress 链接地址对象
   */
  public ConnectionPool(ConnectionPoolConfig poolConfig, InetSocketAddress socketAddress) {
    super(poolConfig, new ConnectionFactory(socketAddress));
    log.trace("Create connection pool with configuration: " + poolConfig + " for: " + socketAddress);
  }
}
