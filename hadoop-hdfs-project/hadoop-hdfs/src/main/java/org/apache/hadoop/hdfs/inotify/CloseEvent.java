package org.apache.hadoop.hdfs.inotify;

public class CloseEvent extends Event {
  private String path;
  private long fileSize;

  public CloseEvent(String path, long fileSize) {
    this.path = path;
    this.fileSize = fileSize;
  }

  public String getPath() {
    return path;
  }

  public long getFileSize() {
    return fileSize;
  }
}
