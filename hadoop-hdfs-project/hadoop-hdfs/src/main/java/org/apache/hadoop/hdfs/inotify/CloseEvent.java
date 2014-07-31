package org.apache.hadoop.hdfs.inotify;

public class CloseEvent {
  private String path;

  public CloseEvent(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }
}
