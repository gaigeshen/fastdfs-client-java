package me.gaigeshen.fastdfs.client.connection;

/**
 * 结果数据处理器，用于处理连接返回的数据
 *
 * @author gaigeshen
 */
@FunctionalInterface
public interface ResultHandler {
  /**
   * 处理结果数据，然后返回是否需要继续接收新数据
   *
   * @param result 结果数据
   * @return 如果本次的结果数据只是部分数据的话，需要指明要继续处理
   */
  boolean handle(byte[] result);
}
