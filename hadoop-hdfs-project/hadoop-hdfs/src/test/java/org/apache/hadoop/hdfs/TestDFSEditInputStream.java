package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Test;

import java.io.IOException;

public class TestDFSEditInputStream {

  @Test
  public void testBasic() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    DFSClient client = new DFSClient(NameNode.getAddress(conf), conf);
    FileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, new Path("/file"), 1024, (short) 1, 0L);
    DFSEditInputStream eis = client.getEditStream();
    client.rename("/file", "/file2", null);
    FSEditLogOp next = null;
    while ((next = eis.next()) == null);
    System.out.println(next);
  }
}
