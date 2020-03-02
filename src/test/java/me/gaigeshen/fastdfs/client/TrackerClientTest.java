package me.gaigeshen.fastdfs.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;

/**
 * @author gaigeshen
 */
@Slf4j
public class TrackerClientTest {

  private TrackerClient trackerClient;

  @Before
  public void init() throws Exception {
    trackerClient = TrackerClient.create(
            new InetSocketAddress[] {
                    new InetSocketAddress("111.229.73.193", 22122)
            }
    );
  }

  @After
  public void destroy() throws Exception {
    trackerClient.close();
  }

  @Test
  public void testUpload() throws Exception {
    StorageServer storeStorage = trackerClient.getStoreStorage();
    String[] groupAndFilename = storeStorage.upload(
            new FileInputStream("C:\\Users\\gaigeshen\\Pictures\\Saved Pictures\\background\\Iceland-Mountain-Waterfall.jpg"),
            769928, "jpg", Collections.emptyMap());
    log.debug("testUpload, uploaded result: " + Arrays.toString(groupAndFilename));
    Assert.assertTrue(ArrayUtils.isNotEmpty(groupAndFilename) && groupAndFilename.length == 2);
  }

  @Test
  public void testGenerateToken() {
    String token = StorageServer.generateToken("M00/00/00/rBEADF5cx-mAKJilAAu_iN43pko886.jpg",
            System.currentTimeMillis(), "1234567890");
    log.debug("testGenerateToken, token: " + token);
    Assert.assertTrue(StringUtils.isNotBlank(token));
  }

  @Test
  public void testGenerateSlaveFilename() {
    String slaveFilename = StorageServer.generateSlaveFilename(
            "M00/00/00/rBEADF5cx-mAKJilAAu_iN43pko886.jpg", "_200x200");
    log.debug("testGenerateSlaveFilename, slave filename: " + slaveFilename);
    Assert.assertTrue(StringUtils.isNotBlank(slaveFilename));
  }
}
