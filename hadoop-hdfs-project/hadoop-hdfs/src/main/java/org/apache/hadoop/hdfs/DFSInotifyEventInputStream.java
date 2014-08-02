package org.apache.hadoop.hdfs;

import com.google.common.collect.Iterators;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventsList;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.io.IOException;
import java.util.Iterator;

public class DFSInotifyEventInputStream {
  private final ClientProtocol namenode;
  private Iterator<Event> it;
  private long lastReadTxid;

  DFSInotifyEventInputStream(ClientProtocol namenode) throws IOException {
    this.namenode = namenode;
    this.it = Iterators.emptyIterator();
    this.lastReadTxid = namenode.getCurrentTxid(); // only consider new txn's
  }

  /**
   * Non-blocking call. Returns null if no new edits are currently available.
   */
  public Event next() throws IOException {
    if (!it.hasNext()) {
      EventsList el = namenode.getEditsFromTxid(lastReadTxid + 1);
      if (el.getLastTxid() != -1) {
        it = el.getEvents().iterator();
        lastReadTxid = el.getLastTxid();
      } else {
        return null;
      }
    }

    return it.next(); // must have a next element at this point
  }

  /**
   * Advance the stream to the NameNode's current position in the
   * edit log. This method can be called if next() throws an IOException,
   * which may indicate that all log segments at the current point in the stream
   * are corrupt.
   */
  public void resync() throws IOException {
    this.lastReadTxid = namenode.getCurrentTxid();
  }
}
