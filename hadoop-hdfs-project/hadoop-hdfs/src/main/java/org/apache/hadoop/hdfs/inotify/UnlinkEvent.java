package org.apache.hadoop.hdfs.inotify;

public class UnlinkEvent extends Event {
  private String path;

  public UnlinkEvent(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }
}
