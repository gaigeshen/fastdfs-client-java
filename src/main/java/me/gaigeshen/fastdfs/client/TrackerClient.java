package me.gaigeshen.fastdfs.client;

import me.gaigeshen.fastdfs.client.connection.ConnectionOptions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.Validate;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 跟踪服务器客户端
 *
 * @author gaigeshen
 */
public class TrackerClient implements Closeable {
  private final TrackerServerGroup serverGroup;

  private TrackerClient(TrackerServerGroup serverGroup) {
    Validate.notNull(serverGroup, "serverGroup");
    this.serverGroup = serverGroup;
  }

  /**
   * 实际上是关闭跟踪服务器组内的所有的跟踪服务器
   *
   * @throws IOException 发生异常
   */
  @Override
  public void close() throws IOException {
    serverGroup.close();
  }

  /**
   * 创建跟踪服务器客户端
   *
   * @param trackerServerAddresses 跟踪服务器的链接地址
   * @return 跟踪服务器客户端
   */
  public static TrackerClient create(InetSocketAddress[] trackerServerAddresses) {
    Validate.notEmpty(trackerServerAddresses, "trackerServerAddresses");
    TrackerServer[] trackerServers = new TrackerServer[trackerServerAddresses.length];
    int index = 0;
    for (InetSocketAddress socketAddress : trackerServerAddresses) {
      trackerServers[index++] = TrackerServer.create(ConnectionOptions.create(socketAddress));
    }
    return new TrackerClient(new TrackerServerGroup(trackerServers));
  }

  /**
   * 获取存储服务器用于存储文件
   *
   * @param groupName 服务器分组名称
   * @return 存储服务器
   */
  public StorageServer getStoreStorage(String groupName) {
    return getTrackerServer().getStoreStorage(groupName);
  }

  /**
   * 获取存储服务器用于存储文件
   *
   * @return 存储服务器
   */
  public StorageServer getStoreStorage() {
    return getTrackerServer().getStoreStorage();
  }

  /**
   * 获取跟踪服务器
   *
   * @return 跟踪服务器
   */
  public TrackerServer getTrackerServer() {
    return serverGroup.getTrackerServer();
  }

  /**
   * 跟踪服务器组
   *
   * @author gaigeshen
   */
  private static class TrackerServerGroup implements Closeable {
    private TrackerServer[] trackerServers;
    private TrackerServer[] validTrackerServers;

    public TrackerServerGroup(TrackerServer[] trackerServers) {
      Validate.notEmpty(trackerServers, "trackerServers");
      this.trackerServers = trackerServers;
      this.validTrackerServers = trackerServers;
    }

    private TrackerServer getTrackerServerInternal() {
      if (validTrackerServers.length == 0) {
        throw new IllegalStateException("No valid trackers");
      }
      int index = RandomUtils.nextInt(0, validTrackerServers.length);
      TrackerServer trackerServer = validTrackerServers[index];
      if (trackerServer.checkServerStatus()) {
        return trackerServer;
      }
      validTrackerServers = ArrayUtils.remove(validTrackerServers, index);
      return getTrackerServerInternal();
    }

    /**
     * 选择某个跟踪服务器
     *
     * @return 跟踪服务器
     */
    public TrackerServer getTrackerServer() {
      validTrackerServers = trackerServers;
      return getTrackerServerInternal();
    }

    @Override
    public void close() throws IOException {
      for (TrackerServer trackerServer : trackerServers) {
        trackerServer.close();
      }
    }
  }
}
