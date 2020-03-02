package me.gaigeshen.fastdfs.client;

import me.gaigeshen.fastdfs.client.connection.ConnectionOptions;
import me.gaigeshen.fastdfs.client.connection.ConnectionPoolConfig;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 存储服务器
 *
 * @author gaigeshen
 */
public class StorageServer extends Server {

  private int storePath;

  /**
   * 创建存储服务器
   *
   * @param poolConfig 连接池配置
   * @param options 链接选项
   * @param storePath 存储路径索引
   */
  private StorageServer(ConnectionPoolConfig poolConfig, ConnectionOptions options, int storePath) {
    super(poolConfig, options);
    this.storePath = storePath;
  }

  /**
   * 创建存储服务器
   *
   * @param poolConfig 连接池配置
   * @param options 链接选项
   * @param storePath 存储路径索引
   * @return 存储服务器
   */
  public static StorageServer create(ConnectionPoolConfig poolConfig, ConnectionOptions options, int storePath) {
    return new StorageServer(poolConfig, options, storePath);
  }

  /**
   * 创建存储服务器，使用默认的连接池配置
   *
   * @param options 链接选项
   * @param storePath 存储路径索引
   * @return 存储服务器
   */
  public static StorageServer create(ConnectionOptions options, int storePath) {
    return new StorageServer(ConnectionPoolConfig.getDefault(), options, storePath);
  }

  /**
   * 返回存储路径索引
   *
   * @return 存储路径索引
   */
  public int getStorePath() {
    return storePath;
  }

  /**
   * 上传文件
   *
   * @param fileStream 文件输入流，会自动关闭
   * @param fileSize 文件大小，字节单位
   * @param fileExt 文件后缀，没有点号
   * @param metadata 文件关联的元数据
   * @return 上传成功后，分组名称和文件名称
   */
  public String[] upload(InputStream fileStream, long fileSize, String fileExt, Map<String, String> metadata) {
    ByteBuffer storedBuffer = ByteBuffer.allocate(1 + 8 + 6);
    storedBuffer.put((byte) storePath);
    storedBuffer.putLong(fileSize);

    byte[] fileExtByteArray = new byte[6];
    if (StringUtils.isNotBlank(fileExt)) {
      byte[] fileExtByteArrayCalculated = fileExt.getBytes(StandardCharsets.UTF_8);
      System.arraycopy(fileExtByteArrayCalculated, 0, fileExtByteArray, 0, Math.min(fileExtByteArrayCalculated.length, 6));
    }
    storedBuffer.put(fileExtByteArray);

    byte[] received = sendAndReceived((byte) 11, storedBuffer.array(), (int) (15 + fileSize), fileStream);

    String groupName = new String(Arrays.copyOfRange(received, 0, 16), StandardCharsets.UTF_8);
    String filename = new String(Arrays.copyOfRange(received, 16, received.length), StandardCharsets.UTF_8);

    if (metadata != null && metadata.size() > 0) {
      overrideOrMergeMetadata(true, groupName, filename, metadata);
    }

    return new String[] { groupName, filename };
  }

  /**
   * 覆盖或者合并文件元数据
   *
   * @param override 是否覆盖，否则合并
   * @param groupName 分组名称
   * @param filename 文件名称，该名称为上传之后的文件名称
   * @param metadata 文件元数据，可以为空对象
   */
  public void overrideOrMergeMetadata(boolean override, String groupName, String filename, Map<String, String> metadata) {
    byte[] groupNameBytes = new byte[16];
    byte[] groupNameBytesCalculated = groupName.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(groupNameBytesCalculated, 0, groupNameBytes, 0, Math.min(groupNameBytesCalculated.length, 16));
    byte[] filenameBytes = filename.getBytes(StandardCharsets.UTF_8);
    byte[] metadataBytes = packMetadata(metadata).getBytes(StandardCharsets.UTF_8);
    ByteBuffer storedBuffer = ByteBuffer.allocate(2 * 8 + 1 + 16 + filenameBytes.length);
    storedBuffer.put(longToBytes(filenameBytes.length));
    storedBuffer.put(longToBytes(metadataBytes.length));
    storedBuffer.put((byte) (override ? 'O' : 'M'));
    storedBuffer.put(groupNameBytes);
    storedBuffer.put(filenameBytes);
    sendAndReceived((byte) 13, storedBuffer.array(), storedBuffer.capacity());
  }

  /**
   * 获取文件的元数据
   *
   * @param groupName 分组名称
   * @param filename 文件名称，该名称为上传之后的文件名称
   * @return 元数据
   */
  public Map<String, String> getMetadata(String groupName, String filename) {
    byte[] groupNameBytes = new byte[16];
    byte[] groupNameBytesCalculated = groupName.getBytes(StandardCharsets.UTF_8);
    System.arraycopy(groupNameBytesCalculated, 0, groupNameBytes, 0, Math.min(groupNameBytesCalculated.length, 16));
    byte[] filenameBytes = filename.getBytes(StandardCharsets.UTF_8);
    ByteBuffer storedBuffer = ByteBuffer.allocate(16 + filenameBytes.length);
    storedBuffer.put(groupNameBytes);
    storedBuffer.put(filenameBytes);
    byte[] received = sendAndReceived((byte) 15, storedBuffer.array(), storedBuffer.capacity());
    return splitMetadata(new String(received, StandardCharsets.UTF_8));
  }

  private Map<String, String> splitMetadata(String metadata) {
    if (StringUtils.isBlank(metadata)) {
      return Collections.emptyMap();
    }
    Map<String, String> result = new HashMap<>();
    String[] keyAndValue = metadata.split("\\u0001");
    for (String kv : keyAndValue) {
      String[] keyValue = kv.split("\\u0002");
      result.put(keyValue[0], keyValue[1]);
    }
    return result;
  }

  private String packMetadata(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    metadata.forEach((k, v) -> {
      if (sb.length() > 0) {
        sb.append("\\u0001");
      }
      sb.append(k).append("\\u0002").append(v);
    });
    return sb.toString();
  }

  /**
   * 生成文件访问令牌
   *
   * @param filename 文件名称
   * @param timestamp 时间戳
   * @param secret 密钥
   * @return 文件访问令牌
   */
  public static String generateToken(String filename, long timestamp, String secret) {
    byte[] bsFilename = filename.getBytes(StandardCharsets.UTF_8);
    byte[] bsKey = secret.getBytes(StandardCharsets.UTF_8);
    byte[] bsTimestamp = (timestamp + "").getBytes(StandardCharsets.UTF_8);
    byte[] buff = new byte[bsFilename.length + bsKey.length + bsTimestamp.length];
    System.arraycopy(bsFilename, 0, buff, 0, bsFilename.length);
    System.arraycopy(bsKey, 0, buff, bsFilename.length, bsKey.length);
    System.arraycopy(bsTimestamp, 0, buff, bsFilename.length + bsKey.length, bsTimestamp.length);
    return DigestUtils.md5Hex(buff);
  }

  /**
   * 生成从文件名称
   *
   * @param masterFilename 主文件名称
   * @param prefixName 前缀名称
   * @return 从文件名称
   */
  public static String generateSlaveFilename(String masterFilename, String prefixName) {
    if (masterFilename.length() < 28 + 6) {
      throw new IllegalArgumentException("masterFilename");
    }
    String ext = "";
    int dotIndex = masterFilename.indexOf('.', masterFilename.length() - (6 + 1));
    if (dotIndex > 0) {
      ext = masterFilename.substring(dotIndex);
    }
    if (dotIndex < 0) {
      return masterFilename + prefixName + ext;
    } else {
      return masterFilename.substring(0, dotIndex) + prefixName + ext;
    }
  }
}
