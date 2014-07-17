package org.apache.hadoop.hdfs;

import com.google.common.collect.Iterators;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.io.IOException;
import java.util.Iterator;

public class DFSEditInputStream {
  private final ClientProtocol namenode;
  private Iterator<FSEditLogOp> it;
  private long lastReadTxid;

  DFSEditInputStream(ClientProtocol namenode) throws IOException {
    this.namenode = namenode;
    this.it = Iterators.emptyIterator();
    this.lastReadTxid = namenode.getCurrentTxid(); // only consider new txn's
  }

  /**
   * Non-blocking call. Returns null if no new edits are currently available.
   * @return
   */
  public FSEditLogOp next() throws IOException {
    // TODO NN failover case?
    if (!it.hasNext()) {
      it = namenode.getEditsFromTxid(lastReadTxid + 1).iterator();
    }
    if (it.hasNext()) {
      FSEditLogOp op = it.next();
      lastReadTxid = op.getTransactionId(); // TODO consider case where there is no txid?
      return op;
    } else {
      return null;
    }
  }
}
