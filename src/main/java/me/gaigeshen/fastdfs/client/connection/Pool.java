package me.gaigeshen.fastdfs.client.connection;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.Closeable;

/**
 * 对象池
 *
 * @author gaigeshen
 */
public class Pool<T> implements Closeable {

  protected GenericObjectPool<T> internalPool;

  /**
   * 创建对象池
   *
   * @param poolConfig 对象池配置
   * @param factory    对象工厂
   */
  public Pool(GenericObjectPoolConfig<T> poolConfig, PooledObjectFactory<T> factory) {
    this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
  }

  @Override
  public void close() {
    destroy();
  }

  public void destroy() {
    // 先关闭对象池，此时不能再次从此对象池中获取对象
    // 但是可以返还对象和失效对象，那些返还的对象将会被销毁
    internalPool.close();
    // 清空空闲的对象，那些被清空的对象将会被销毁
    internalPool.clear();
  }

  public boolean isClosed() {
    return this.internalPool.isClosed();
  }

  /**
   * 从对象池获取对象
   *
   * @return 对象
   */
  public T getObject() {
    try {
      return internalPool.borrowObject();
    } catch (Exception e) {
      throw new IllegalStateException("Could not get object from pool", e);
    }
  }

  /**
   * 归还对象给对象池
   *
   * @param object 对象
   */
  public void returnObject(T object) {
    try {
      internalPool.returnObject(object);
    } catch (Exception e) {
      throw new IllegalStateException("Could not return the object to the pool", e);
    }
  }

  /**
   * 使对象过期并移除自对象池，最后会被销毁
   *
   * @param object 对象
   */
  public void invalidateObject(T object) {
    try {
      internalPool.invalidateObject(object);
    } catch (Exception e) {
      throw new IllegalStateException("Could not invalidate the object to the pool", e);
    }
  }

  /**
   * 添加指定数量的新对象
   *
   * @param count 对象数量
   */
  public void addObjects(int count) {
    try {
      for (int i = 0; i < count; i++) {
        this.internalPool.addObject();
      }
    } catch (Exception e) {
      throw new IllegalStateException("Error trying to add idle objects", e);
    }
  }

}
