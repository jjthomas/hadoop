package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;

import java.util.List;

public class CreateEvent extends Event {

  public static enum INodeType {
    FILE, DIRECTORY, SYMLINK;
  }

  private INodeType type;
  private String path;
  private long ctime;
  private int replication;
  private String ownerName;
  private String groupName;
  private FsPermission perms;
  private String symlinkTarget;

  public static class Builder {
    private INodeType type;
    private String path;
    private long ctime;
    private int replication;
    private String ownerName;
    private String groupName;
    private FsPermission perms;
    private String symlinkTarget;

    public Builder type(INodeType type) {
      this.type = type;
      return this;
    }

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder ctime(long ctime) {
      this.ctime = ctime;
      return this;
    }

    public Builder replication(int replication) {
      this.replication = replication;
      return this;
    }

    public Builder ownerName(String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    public Builder groupName(String groupName) {
      this.groupName = groupName;
      return this;
    }

    public Builder perms(FsPermission perms) {
      this.perms = perms;
      return this;
    }

    public Builder symlinkTarget(String symlinkTarget) {
      this.symlinkTarget = symlinkTarget;
      return this;
    }

    public CreateEvent build() {
      return new CreateEvent(this);
    }
  }

  private CreateEvent(Builder b) {
    this.type = b.type;
    this.path = b.path;
    this.ctime = b.ctime;
    this.replication = b.replication;
    this.ownerName = b.ownerName;
    this.groupName = b.groupName;
    this.perms = b.perms;
    this.symlinkTarget = b.symlinkTarget;
  }

  public INodeType getType() {
    return type;
  }

  public String getPath() {
    return path;
  }

  public long getCtime() {
    return ctime;
  }

  public int getReplication() {
    return replication;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public String getGroupName() {
    return groupName;
  }

  public FsPermission getPerms() {
    return perms;
  }

  public String getSymlinkTarget() {
    return symlinkTarget;
  }
}
