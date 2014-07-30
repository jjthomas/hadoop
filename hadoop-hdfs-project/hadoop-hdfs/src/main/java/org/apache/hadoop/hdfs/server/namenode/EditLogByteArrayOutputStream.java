package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.IOException;

@InterfaceAudience.Private
public class EditLogByteArrayOutputStream {
  private DataOutputBuffer buf;
  private FSEditLogOp.Writer writer;

  public EditLogByteArrayOutputStream(int layoutVersion) throws IOException {
    buf = new DataOutputBuffer();
    buf.writeInt(layoutVersion);
    LayoutFlags.write(buf);
    writer = new FSEditLogOp.Writer(buf);
  }

  public void write(FSEditLogOp op) throws IOException {
    writer.writeOp(op);
  }

  public void writeRaw(byte[] bytes, int offset, int length) throws IOException {
    buf.write(bytes, offset, length);
  }

  public byte[] getData() {
    return buf.getData();
  }

  public int getDataLength() {
    return buf.getLength();
  }
}
