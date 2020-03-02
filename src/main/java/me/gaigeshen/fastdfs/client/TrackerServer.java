package me.gaigeshen.fastdfs.client;

import me.gaigeshen.fastdfs.client.connection.ConnectionOptions;
import me.gaigeshen.fastdfs.client.connection.ConnectionPoolConfig;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 跟踪服务器
 *
 * @author gaigeshen
 */
public class TrackerServer extends Server {

  private final Set<StorageServer> storageServers = new HashSet<>();

  /**
   * 创建跟踪服务器
   *
   * @param poolConfig 链接对象池配置
   * @param options 链接选择
   */
  private TrackerServer(ConnectionPoolConfig poolConfig, ConnectionOptions options) {
    super(poolConfig, options);
  }

  /**
   * 创建跟踪服务器，采用默认的链接对象池配置
   *
   * @param options 链接选择
   * @return 跟踪服务器
   */
  public static TrackerServer create(ConnectionOptions options) {
    return new TrackerServer(ConnectionPoolConfig.getDefault(), options);
  }

  /**
   * 通过此跟踪服务器获取到的存储服务器都需要调用此方法，然后再返回
   *
   * @param storageServer 存储服务器
   * @return 获取到的跟踪服务器
   */
  private StorageServer recordStorageServer(StorageServer storageServer) {
    storageServers.add(storageServer);
    return storageServer;
  }

  /**
   * 关闭跟踪服务器，将会关闭从中获取的所有存储服务器，所以在系统关闭的时候只需要调用此方法即可
   *
   * @throws IOException 关闭的时候发生异常
   */
  @Override
  public void close() throws IOException {
    // 关闭自身
    super.close();
    // 关闭所有的存储服务器
    Iterator<StorageServer> iterator = storageServers.iterator();
    while (iterator.hasNext()) {
      StorageServer storageServer = iterator.next();
      iterator.remove();
      storageServer.close();
    }
  }

  public StorageServer getUpdateStorage(String groupName, String filename) {
    return null;
  }

  public StorageServer getFetchStorage(String groupName, String filename) {
    return null;
  }

  private StorageServer getStoredStorage(byte command, String groupName, String filename) {
    byte[] groupNameBytes = new byte[16];
    byte[] groupNameBytesCalculated = groupName.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(groupNameBytesCalculated, 0, groupNameBytes, 0, Math.min(groupNameBytesCalculated.length, 16));

    byte[] filenameBytes = filename.getBytes(StandardCharsets.UTF_8);

    ByteBuffer storedBuffer = ByteBuffer.allocate(16 + filenameBytes.length);
    storedBuffer.put(groupNameBytes);
    storedBuffer.put(filenameBytes);
    byte[] bodyReceived = sendAndReceived(command, storedBuffer.array(), storedBuffer.capacity());

    String host = new String(bodyReceived, 16, 16 - 1).trim();
    int port = (int) bytesToLong(bodyReceived, 31);

    return recordStorageServer(StorageServer.create(ConnectionOptions.create(host, port), 0));
  }

  /**
   * 获取存储服务器
   *
   * @return 存储服务器
   */
  public StorageServer getStoreStorage() {
    return getStoreStorage(null);
  }

  /**
   * 获取存储服务器
   *
   * @param groupName 组名称，可以为空
   * @return 存储服务器
   */
  public StorageServer getStoreStorage(String groupName) {
    byte[] body = StringUtils.isBlank(groupName)
            ? null : groupName.getBytes(StandardCharsets.UTF_8);
    int bodyMaxLength = body != null ? 16 : 0;
    byte command = (byte) (StringUtils.isBlank(groupName) ? 101 : 104);

    byte[] bodyReceived = body != null
            ? sendAndReceived(command, body, bodyMaxLength)
            : sendAndReceived(command);

    String host = new String(Arrays.copyOfRange(bodyReceived, 16, 32));
    long port = ByteBuffer.wrap(bodyReceived, 31, 8).getLong();
    byte storePath = ByteBuffer.wrap(bodyReceived, 39, 1).get();

    return recordStorageServer(StorageServer.create(ConnectionOptions.create("111.229.73.193", (int) port), storePath));
  }
}
