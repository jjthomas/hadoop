package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class TestRWCycles {

  public static void createFile(FileSystem fs, Path fileName, long fileLen)
      throws IOException {
    int bufferLen = 1024;
    assert bufferLen > 0;
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " +
          fileName.getParent().toString());
    }
    FSDataOutputStream out = null;
    try {
      out = fs.create(fileName, true, fs.getConf()
              .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
          (short) 1, fs.getDefaultBlockSize(fileName));
      if (fileLen > 0) {
        byte[] toWrite = new byte[bufferLen];
        // Random rb = new Random(0);
        long bytesToWrite = fileLen;
        while (bytesToWrite>0) {
          // rb.nextBytes(toWrite);
          int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen
              : (int) bytesToWrite;

          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster c = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2)
        .format(true)
        .build();
    FileSystem fs = c.getFileSystem();
    createFile(fs, new Path("/large"), Integer.parseInt(System.getProperty(
        "test.cycles.mb")) * 1024 * 1024);
  }
}