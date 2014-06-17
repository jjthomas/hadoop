package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestDatanodeLayoutUpgrade {
  private static final String HADOOP_DATANODE_DIR_TXT =
      "hadoop-datanode-dir.txt";
  private static final String HADOOP24_DATANODE = "hadoop-24-datanode-dir.tgz";

  @Test
  // Upgrade from LDir-based layout to block ID-based layout -- change described
  // in HDFS-6482
  public void testUpgradeToIdBasedLayout() throws IOException {
    TestDFSUpgradeFromImage upgrade = new TestDFSUpgradeFromImage();
    upgrade.unpackStorage(HADOOP24_DATANODE, HADOOP_DATANODE_DIR_TXT);
    Configuration conf = new Configuration(TestDFSUpgradeFromImage.upgradeConf);
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
        System.getProperty("test.build.data") + File.separator +
            "dfs" + File.separator + "data");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        System.getProperty("test.build.data") + File.separator +
            "dfs" + File.separator + "name");
    upgrade.upgradeAndVerify(new MiniDFSCluster.Builder(conf).numDataNodes(1)
    .manageDataDfsDirs(false).manageNameDfsDirs(false));
  }
}
