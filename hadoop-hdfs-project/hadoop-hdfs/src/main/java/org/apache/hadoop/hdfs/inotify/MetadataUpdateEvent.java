package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;

import java.util.List;

public class MetadataUpdateEvent extends Event {
  private String path;
  private long mtime;
  private long atime;
  private int replication;
  private String ownerName;
  private String groupName;
  private FsPermission perms;
  private List<AclEntry> acls;
  private List<XAttr> xAttrs;
  private boolean xAttrsRemoved;

  public static class Builder {
    private String path;
    private long mtime;
    private long atime;
    private int replication;
    private String ownerName;
    private String groupName;
    private FsPermission perms;
    private List<AclEntry> acls;
    private List<XAttr> xAttrs;
    private boolean xAttrsRemoved;

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder mtime(long mtime) {
      this.mtime = mtime;
      return this;
    }

    public Builder atime(long atime) {
      this.atime = atime;
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

    public Builder acls(List<AclEntry> acls) {
      this.acls = acls;
      return this;
    }

    public Builder xAttrs(List<XAttr> xAttrs) {
      this.xAttrs = xAttrs;
      return this;
    }

    public Builder xAttrsRemoved(boolean xAttrsRemoved) {
      this.xAttrsRemoved = xAttrsRemoved;
      return this;
    }

    public MetadataUpdateEvent build() {
      return new MetadataUpdateEvent(this);
    }
  }

  private MetadataUpdateEvent(Builder b) {
    this.path = b.path;
    this.mtime = b.mtime;
    this.atime = b.atime;
    this.replication = b.replication;
    this.ownerName = b.ownerName;
    this.groupName = b.groupName;
    this.perms = b.perms;
    this.acls = b.acls;
    this.xAttrs = b.xAttrs;
    this.xAttrsRemoved = b.xAttrsRemoved;
  }

  public String getPath() {
    return path;
  }

  public long getMtime() {
    return mtime;
  }

  public long getAtime() {
    return atime;
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

  public List<AclEntry> getAcls() {
    return acls;
  }

  public List<XAttr> getxAttrs() {
    return xAttrs;
  }

  public boolean isxAttrsRemoved() {
    return xAttrsRemoved;
  }

}
