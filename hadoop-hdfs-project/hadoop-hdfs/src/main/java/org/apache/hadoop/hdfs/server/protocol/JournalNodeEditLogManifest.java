package org.apache.hadoop.hdfs.server.protocol;

import java.util.List;

public class JournalNodeEditLogManifest extends RemoteEditLogManifest {
  private long lastWrittenEpoch;

  public JournalNodeEditLogManifest(List<RemoteEditLog> logs,
      long lastWrittenEpoch) {
    super(logs);
    this.lastWrittenEpoch = lastWrittenEpoch;
  }

  public long getLastWrittenEpoch() {
    return lastWrittenEpoch;
  }
}
