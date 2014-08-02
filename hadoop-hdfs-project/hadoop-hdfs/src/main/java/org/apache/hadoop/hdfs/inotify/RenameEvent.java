package org.apache.hadoop.hdfs.inotify;

public class RenameEvent extends Event {
  private String srcPath;
  private String dstPath;

  public RenameEvent(String srcPath, String dstPath) {
    this.srcPath = srcPath;
    this.dstPath = dstPath;
  }

  public String getSrcPath() {
    return srcPath;
  }

  public String getDstPath() {
    return dstPath;
  }
}
