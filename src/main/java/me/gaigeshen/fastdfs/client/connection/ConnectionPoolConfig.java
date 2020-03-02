package me.gaigeshen.fastdfs.client.connection;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * 对象池的配置
 *
 * @author gaigeshen
 */
public class ConnectionPoolConfig extends GenericObjectPoolConfig<Connection> {
  /**
   * @author gaigeshen
   */
  private static class DefaultPoolConfigHolder {
    private static final ConnectionPoolConfig INSTANCE = new ConnectionPoolConfig();
  }

  /**
   * 返回默认的对象池的配置
   *
   * @return 默认的对象池的配置
   */
  public static ConnectionPoolConfig getDefault() {
    return DefaultPoolConfigHolder.INSTANCE;
  }
}
