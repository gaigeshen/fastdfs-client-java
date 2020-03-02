package me.gaigeshen.fastdfs.client;

/**
 * @author gaigeshen
 */
public class StorageClient {

  private final TrackerClient trackerClient;

  public StorageClient(TrackerClient trackerClient) {
    this.trackerClient = trackerClient;
  }

  public StorageServer getUpdateStorage() {
    return null;
  }

  public StorageServer getFetchStorage() {
    return null;
  }

  public StorageServer getStoreStorage(String groupName) {
    return trackerClient.getStoreStorage(groupName);
  }

  public StorageServer getStoreStorage() {
    return trackerClient.getStoreStorage();
  }

}
