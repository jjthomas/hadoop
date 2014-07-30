package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.protocol.LayoutFlags;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class EditLogByteArrayInputStream {
  private DataInputStream in;
  private FSEditLogOp.Reader reader;

  public EditLogByteArrayInputStream(byte[] input) throws IOException {
    FSEditLogLoader.PositionTrackingInputStream limiter =
        new FSEditLogLoader.PositionTrackingInputStream(new
            ByteArrayInputStream(input));
    in = new DataInputStream(limiter);
    int logVersion = in.readInt();
    LayoutFlags.read(in);
    reader = new FSEditLogOp.Reader(in, limiter, logVersion);
  }

  public FSEditLogOp nextOp() throws IOException {
    return reader.readOp(false);
  }
}
