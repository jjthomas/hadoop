package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.EditLogByteArrayInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.io.IOException;

public class DFSEditInputStream {
  private final ClientProtocol namenode;
  private EditLogByteArrayInputStream in;
  private long lastReadTxid;
  private static final Log LOG = LogFactory.getLog(DFSEditInputStream.class);


  DFSEditInputStream(ClientProtocol namenode) throws IOException {
    this.namenode = namenode;
    this.lastReadTxid = namenode.getCurrentTxid(); // only consider new txn's
  }

  /**
   * Non-blocking call. Returns null if no new edits are currently available.
   * @return
   */
  public FSEditLogOp next() {
    // TODO NN failover case?
    FSEditLogOp next = null;
    boolean zeroLengthEdits = false;
    try {
      if (in == null) {
        byte[] edits = namenode.getEditsFromTxid(lastReadTxid + 1);
        zeroLengthEdits = edits.length == 0;
        in = new EditLogByteArrayInputStream(edits);
      }
      next = in.nextOp();
    } catch (IOException e) {
      if (!zeroLengthEdits) {
        LOG.info("Unable to read edits returned from NameNode ... will retry " +
            "on next call to next()", e);
      }
      in = null;
      return null;
    }

    if (next == null) {
      in = null;
      return null;
    }

    lastReadTxid = next.getTransactionId(); // TODO consider case where there is no txid?
    return next;
  }
}
