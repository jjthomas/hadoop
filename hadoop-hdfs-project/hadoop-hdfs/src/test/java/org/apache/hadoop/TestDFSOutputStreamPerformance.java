package org.apache.hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;

public class TestDFSOutputStreamPerformance {
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static List<String> msgs;

  private static final int BYTES_PER_TEST = 8 * 1024 * 1024;
  private static final int MAX_LOG_SIZE = 10;

  @Before
  public void setup() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BYTES_PER_TEST * 2);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY,
        BYTES_PER_TEST * 2);
    cluster = new MiniDFSCluster.Builder(conf).build();
    fs = cluster.getFileSystem();
    msgs = Lists.newArrayList();
  }

  @Test
  public void test() throws IOException {
    for (int i = 0; i <= MAX_LOG_SIZE; i++) {
      runPerfTest(i, false);
      runPerfTest(i, true);
    }
  }

  @After
  public void printOutput() {
    for (String msg : msgs) {
      System.out.println(msg);
    }
    cluster.shutdown();
  }

  public void runPerfTest(int logBufSize, boolean useBufferedStream)
      throws IOException {
    OutputStream os = fs.create(new Path("/file" + logBufSize +
        useBufferedStream), true, fs.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096));
    if (useBufferedStream) {
      os = new BufferedOutputStream(os);
    }

    int bufSize = 1 << logBufSize;
    byte[] data = new byte[bufSize];
    new Random(0L).nextBytes(data);
    long now = Time.monotonicNow();
    for (int i = 0; i < BYTES_PER_TEST / bufSize; i++) {
      os.write(data, 0, bufSize);
    }
    msgs.add("Time for " + logBufSize + " buffer, useBufferedStream=" +
      useBufferedStream + ": " + (Time.monotonicNow() - now) + "ms");
    os.close();

  }
}
