package org.apache.hadoop.hdfs.inotify;

public class ReopenEvent {
  private String path;

  public ReopenEvent(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }
}
