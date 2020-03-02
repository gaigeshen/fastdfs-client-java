package me.gaigeshen.fastdfs.client.connection;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.net.InetSocketAddress;

/**
 * 连接对象工厂，生成的连接对象为可池化的对象
 *
 * @author gaigeshen
 */
@Slf4j
public class ConnectionFactory implements PooledObjectFactory<Connection> {

  private final InetSocketAddress socketAddress;

  /**
   * 创建连接对象工厂
   *
   * @param socketAddress 需要此连接地址对象，用于生成连接对象
   */
  public ConnectionFactory(InetSocketAddress socketAddress) {
    Validate.notNull(socketAddress, "socketAddress");
    this.socketAddress = socketAddress;
  }

  @Override
  public PooledObject<Connection> makeObject() throws Exception {
    Connection connection = new Connection(socketAddress);
    log.trace("Make connection: " + connection);
    return new DefaultPooledObject<>(connection);
  }

  @Override
  public void destroyObject(PooledObject<Connection> p) throws Exception {
    Connection connection = p.getObject();
    log.trace("Destroy connection: " + connection);
    connection.close();
  }

  @Override
  public boolean validateObject(PooledObject<Connection> p) {
    return p.getObject().isConnected();
  }

  @Override
  public void activateObject(PooledObject<Connection> p) throws Exception {
    // 没有实现
  }

  @Override
  public void passivateObject(PooledObject<Connection> p) throws Exception {
    // 没有实现
  }
}
