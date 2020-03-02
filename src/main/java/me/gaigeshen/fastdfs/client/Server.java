package me.gaigeshen.fastdfs.client;

import lombok.extern.slf4j.Slf4j;
import me.gaigeshen.fastdfs.client.connection.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务器抽象
 *
 * <pre>
 * Protocol data package: body_length(8) + command(1) + status(1) + body(N >= 0)
 * </pre>
 *
 * @author gaigeshen
 */
@Slf4j
public abstract class Server implements Closeable {

  private final static int DEFAULT_GET_CONNECTION_MAX_RETRY_TIMES = 3;

  protected final static byte STATUS_SUCCESS = 0;
  protected final static byte CMD_QUIT = 82;
  protected final static byte CMD_ACTIVE_TEST = 111;

  private final static Map<InetSocketAddress, ConnectionPool> POOL = new ConcurrentHashMap<>();

  private final InetSocketAddress socketAddress;

  /**
   * 创建服务器抽象，服务器可以重复创建，内部对应的连接池不会因为重复创建服务器而改变，
   * 每个服务器都必须有对应的链接地址对象，该对象用于关联连接池对象
   *
   * @param poolConfig 连接池配置
   * @param options 链接选项，如果此前已经创建过同样链接地址对象的服务器，那么此次创建使用的该链接选项不会生效
   */
  protected Server(ConnectionPoolConfig poolConfig, ConnectionOptions options) {
    this.socketAddress = options.getSocketAddress();
    POOL.computeIfAbsent(socketAddress, k -> new ConnectionPool(poolConfig, options.getSocketAddress()));
  }

  @Override
  public void close() throws IOException {
    ConnectionPool connectionPool = POOL.remove(socketAddress);
    if (connectionPool != null) {
      log.trace("Close server: " + this);
      connectionPool.close();
    }
  }

  @Override
  public int hashCode() {
    return socketAddress.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || obj.getClass().equals(this.getClass())) {
      return false;
    }
    return socketAddress.equals(((Server) obj).socketAddress);
  }

  /**
   * 记录发送的数据，打印日志
   *
   * @param dataToSend 发送的数据
   */
  private void log(byte[] dataToSend) {
    log(dataToSend, null);
  }

  /**
   * 记录发送和接收的数据，打印日志
   *
   * @param dataToSend 发送的数据
   * @param receivedData 接收到的数据，此参数可选
   */
  private void log(byte[] dataToSend, byte[] receivedData) {
    StringBuilder builder = new StringBuilder("Server: %s, ");
    builder.append("send: %s, ");
    if (receivedData != null) {
      builder.append("received: %s");
    }
    String logStr = receivedData != null
      ? String.format(builder.toString(), this, toHex(dataToSend), toHex(receivedData))
      : String.format(builder.toString(), this, toHex(dataToSend));
    log.trace(logStr);
  }

  /**
   * 发送数据然后返回接收的数据
   *
   * @param command 指令
   * @param body 数据体
   * @param bodyMaxLength 数据体的最大长度，不一定直接等于数据体的长度，务必包含剩余需要发送的数据长度
   * @param remainingDataSource 如果有其他的剩余数据需要发送，传递此参数，使用完毕之后会自动关闭该输入流
   * @return 接收的数据，只是数据体
   */
  protected byte[] sendAndReceived(byte command, byte[] body, int bodyMaxLength, InputStream remainingDataSource) {
    byte[] dataToSend = prepareDataToSend(command, body, bodyMaxLength, false);
    Connection connection = getConnectionValid();
    ReceivedResultData received = new ReceivedResultData();
    try {
      connection.sendAndReceived(dataToSend, RemainingDataSupplier.create(remainingDataSource), received::addBytes);
    } catch (IOException e) {
      log(dataToSend);
      throw new IllegalStateException(e);
    } finally {
      releaseConnection(connection);
    }
    log(dataToSend, received.getBytes());
    ResultData resultData = received.parseToResultData();
    if (!resultData.isSuccess()) {
      throw new IllegalStateException("Result status: " + resultData.getStatus());
    }
    return resultData.getBody();
  }

  /**
   * 发送数据然后返回接收的数据，没有任何其他数据，只有指令
   *
   * @param command 指令
   * @return 接收的数据，只是数据体
   */
  protected byte[] sendAndReceived(byte command) {
    return sendAndReceived(command, null, 0);
  }

  /**
   * 发送数据然后返回接收的数据
   *
   * @param command 指令
   * @param body 数据体
   * @param bodyMaxLength 数据体的最大长度，不一定直接等于数据体的长度
   * @return 接收的数据，只是数据体
   */
  protected byte[] sendAndReceived(byte command, byte[] body, int bodyMaxLength) {
    Connection connection = getConnectionValid();
    try {
      return sendAndReceived(connection, command, body, bodyMaxLength);
    } finally {
      releaseConnection(connection);
    }
  }

  /**
   * 发送数据然后返回接收的数据
   *
   * @param connection 链接对象
   * @param command 指令
   * @param body 数据体
   * @param bodyMaxLength 数据体的最大长度，不一定直接等于数据体的长度
   * @return 收的数据，只是数据体
   */
  protected byte[] sendAndReceived(Connection connection, byte command, byte[] body, int bodyMaxLength) {
    byte[] dataToSend = prepareDataToSend(command, body, bodyMaxLength);
    ReceivedResultData received = new ReceivedResultData();
    try {
      connection.sendAndReceived(dataToSend, received::addBytes);
    } catch (IOException e) {
      log(dataToSend);
      throw new IllegalStateException(e);
    }
    log(dataToSend, received.getBytes());
    ResultData resultData = received.parseToResultData();
    if (!resultData.isSuccess()) {
      throw new IllegalStateException("Result status: " + resultData.getStatus());
    }
    return resultData.getBody();
}

  /**
   * 发送数据，没有返回数据
   *
   * @param command 只有指令
   */
  protected void send(byte command) {
    Connection connection = getConnectionValid();
    try {
      send(connection, command);
    } finally {
      releaseConnection(connection);
    }
  }

  /**
   * 发送数据，没有返回数据
   *
   * @param connection 链接对象
   * @param command 指令
   */
  protected void send(Connection connection, byte command) {
    byte[] dataToSend = prepareDataToSend(command, null, 0);
    log(dataToSend, null);
    try {
      connection.send(dataToSend);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * 检查服务器的状态，只有在有效的状态情况下，才能正常使用
   *
   * @return 状态是否有效
   */
  protected boolean checkServerStatus() {
    try {
      return getConnectionValid() != null;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * 获取链接对象，此链接对象有效，使用默认的重试次数
   *
   * @return 返回有效的链接对象
   */
  private Connection getConnectionValid() {
    return getConnectionValid(DEFAULT_GET_CONNECTION_MAX_RETRY_TIMES);
  }

  /**
   * 获取链接对象，此链接对象有效
   *
   * @param maxRetryTimes 重试次数，达到此重试次数则抛出异常
   * @return 返回有效的链接对象
   */
  private Connection getConnectionValid(int maxRetryTimes) {
    if (maxRetryTimes <= 0) {
      throw new IllegalStateException("Could not get connections, retried max times");
    }
    // 组装数据，该数据用于检查稍后获取的链接对象是否有效
    byte[] dataToSend = prepareDataToSend(CMD_ACTIVE_TEST, null, 0);
    ReceivedResultData received = new ReceivedResultData();
    Connection connection = getConnectionPool().getObject();
    // 发送检查数据
    try {
      connection.sendAndReceived(dataToSend, received::addBytes);
    } catch (IOException e) {
      log(dataToSend);
      // 发生异常消耗重试次数
      return getConnectionValid(--maxRetryTimes);
    }
    log(dataToSend, received.getBytes());
    // 检查结果为有效，直接返回该链接对象
    if (received.parseToResultData().isSuccess()) {
      return connection;
    }
    // 检查结果无效，将获取到的链接对象置为失效，然后继续获取下个有效的链接对象
    getConnectionPool().invalidateObject(connection);
    return getConnectionValid(--maxRetryTimes);
  }

  /**
   * 获取链接对象，该链接未进行有效的检查
   *
   * @return 链接对象
   */
  private Connection getConnection() {
    return getConnectionPool().getObject();
  }

  /**
   * 释放链接对象
   *
   * @param connection 链接对象
   */
  private void releaseConnection(Connection connection) {
    if (connection != null) {
      getConnectionPool().returnObject(connection);
    }
  }

  /**
   * 关闭链接对象
   *
   * @param connection 链接对象
   */
  private void closeConnection(Connection connection) {
    send(connection, CMD_QUIT);
    getConnectionPool().invalidateObject(connection);
  }

  /**
   * 获取链接对象池，对象池获取是根据当前服务器关联的链接地址来获取的
   *
   * @return 链接对象池
   */
  private ConnectionPool getConnectionPool() {
    ConnectionPool pool = POOL.get(socketAddress);
    Validate.notNull(pool, "Connection pool is null of this server: " + socketAddress);
    return pool;
  }

  /**
   * 准备要发送的数据
   *
   * @param command 指令
   * @param body 数据体
   * @param bodyMaxLength 数据体最大长度
   * @return 要发送的数据
   */
  private byte[] prepareDataToSend(byte command, byte[] body, int bodyMaxLength) {
    return prepareDataToSend(STATUS_SUCCESS, command, body, bodyMaxLength);
  }

  /**
   * 准备要发送的数据
   *
   * @param status 状态码
   * @param command 指令
   * @param body 数据体
   * @param bodyMaxLength 数据体最大长度
   * @return 要发送的数据
   */
  private byte[] prepareDataToSend(byte status, byte command, byte[] body, int bodyMaxLength) {
    return prepareDataToSend(status, command, body, bodyMaxLength, true);
  }

  /**
   * 准备要发送的数据
   *
   * @param command 指令
   * @param body 数据体
   * @param bodyMaxLength 数据体最大长度
   * @param maxLength 发送的数据长度是否取最大长度，如果不取最大长度，则准备的数据长度取数据体的真实长度
   * @return 要发送的数据
   */
  private byte[] prepareDataToSend(byte command, byte[] body, int bodyMaxLength, boolean maxLength) {
    return prepareDataToSend(STATUS_SUCCESS, command, body, bodyMaxLength, maxLength);
  }

  /**
   * 准备要发送的数据
   *
   * @param status 状态码
   * @param command 指令
   * @param body 数据体
   * @param bodyMaxLength 数据体最大长度
   * @param maxLength 发送的数据长度是否取最大长度，如果不取最大长度，则准备的数据长度取数据体的真实长度
   * @return 要发送的数据
   */
  private byte[] prepareDataToSend(byte status, byte command, byte[] body, int bodyMaxLength, boolean maxLength) {
    ByteBuffer buffer = ByteBuffer.allocate(10 + (maxLength ? bodyMaxLength : (body != null ? body.length : 0)));
    buffer.putLong(bodyMaxLength);
    buffer.put(command);
    buffer.put(status);
    if (body != null) {
      byte[] allBodyBytes = new byte[maxLength ? bodyMaxLength : body.length];
      System.arraycopy(body, 0, allBodyBytes, 0, body.length);
      buffer.put(allBodyBytes);
    }
    return buffer.array();
  }

  /**
   * 将长整数转为字节数组
   *
   * @param number 长整数
   * @return 字节数组
   */
  protected byte[] longToBytes(long number) {
    return ByteBuffer.allocate(8).putLong(number).array();
  }

  /**
   * 将字节数组转为长整数
   *
   * @param bytes 字节数组
   * @param offset 偏移量，前多少个字节忽略
   * @return 长整数
   */
  protected long bytesToLong(byte[] bytes, int offset) {
    Validate.isTrue(bytes != null && bytes.length - offset >= 8, "bytes's length invalid");
    return ByteBuffer.wrap(bytes, offset, bytes.length - offset).getLong();
  }

  /**
   * 将字节数组转为长整数
   *
   * @param bytes 字节数组
   * @return 长整数
   */
  protected long bytesToLong(byte[] bytes) {
    Validate.isTrue(bytes != null && bytes.length == 8, "bytes's length must be 8");
    return ByteBuffer.wrap(bytes).getLong();
  }

  /**
   * 将字节数组转为十六进制字符串，字节之间采用空格相间
   *
   * @param data 字节数组数据
   * @return 十六进制字符串
   */
  protected String toHex(byte[] data) {
    return toHex(data, true);
  }

  /**
   * 将字节数组转为十六进制字符串
   *
   * @param data 字节数组数据
   * @param spacing 是否在每个字节之间加入空格
   * @return 十六进制字符串
   */
  protected String toHex(byte[] data, boolean spacing) {
    if (ArrayUtils.isEmpty(data)) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for (byte b : data) {
      if (spacing) {
        builder.append(" ");
      }
      builder.append(toHex(b));
    }
    if (spacing) {
      return builder.substring(1);
    }
    return builder.toString();
  }

  /**
   * 将字节转为十六进制字符串
   *
   * @param data 字节数据
   * @return 十六进制字符串
   */
  protected String toHex(byte data) {
    String result = Integer.toHexString(data & 0xff);
    if (result.length() != 2) {
      return "0" + result;
    }
    return result;
  }

  @Override
  public String toString() {
    return String.format("Server[%s]", socketAddress);
  }

  /**
   * 已接收的从服务器返回的数据
   *
   * @author gaigeshen
   */
  private class ReceivedResultData {
    private byte[] received = new byte[0];

    /**
     * 添加新的数据，然后返回是否需要继续添加缺少的数据
     *
     * @param bytes 添加新的数据
     * @return 是否需要继续添加缺少的数据
     */
    public boolean addBytes(byte[] bytes) {
      received = ArrayUtils.addAll(received, bytes);
      // 接受的数据不完整，连头部数据都不足
      if (received.length < 10) {
        return true;
      }
      // 这部分的数据标识数据体的长度
      byte[] bBodyLength = Arrays.copyOfRange(received, 0, 8);
      long bodyLength = ByteBuffer.wrap(bBodyLength).getLong();
      // 整个数据的长度必须等于头部数据加上数据体的长度
      return received.length < 10 + bodyLength;
    }

    /**
     * 返回已接收的数据
     *
     * @return 已接收的数据
     */
    public byte[] getBytes() {
      return received;
    }

    /**
     * 直接转为结构化的数据
     *
     * @return 结构化的数据
     */
    public ResultData parseToResultData() {
      return ResultData.create(received);
    }
  }

  /**
   *
   * 结构化的，转化后的从服务器返回的数据
   *
   * @author gaigeshen
   */
  private static class ResultData {
    private final byte command;
    private final byte status;
    private final byte[] body;

    private ResultData(byte command, byte status, byte[] body) {
      this.command = command;
      this.status = status;
      this.body = body;
    }

    /**
     * 创建此对象，传入的数据为全部的数据，包含头部数据和数据体数据
     *
     * @param data 全部的数据
     * @return 此对象
     */
    private static ResultData create(byte[] data) {
      byte[] header = Arrays.copyOfRange(data, 0, 10);
      byte[] bBodyLength = Arrays.copyOfRange(header, 0, 8);
      byte command = header[8];
      byte status = header[9];
      long bodyLength = ByteBuffer.wrap(bBodyLength).getLong();
      return new ResultData(command, status, bodyLength > 0 ? Arrays.copyOfRange(data, 10, data.length): null);
    }

    /**
     * 返回是否是成功的数据
     *
     * @return 是否是成功的数据
     */
    public boolean isSuccess() {
      return status == STATUS_SUCCESS;
    }

    /**
     * 传输数据体至指定的输出流
     *
     * @param out 输出流，不会关闭
     * @throws IOException 发生异常
     */
    public void transferBodyTo(OutputStream out) throws IOException {
      Validate.notNull(body, "No body found");
      out.write(body);
    }

    /**
     * 返回指令
     *
     * @return 指令
     */
    public byte getCommand() {
      return command;
    }

    /**
     * 返回状态码
     *
     * @return 状态码
     */
    public byte getStatus() {
      return status;
    }

    /**
     * 返回数据体
     *
     * @return 数据体
     */
    public byte[] getBody() {
      return body;
    }
  }
}
