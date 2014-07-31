package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;

import java.util.List;

public class MetadataUpdateEvent {
  private String path;
  private long inodeId;
  private long mtime;
  private long atime;
  private int replication;
  private String ownerName;
  private String groupName;
  private String symlinkTarget;
  private List<AclEntry> acls;
  private List<XAttr> xAttrs;
  private boolean xAttrsRemoved;

  public static class Builder {
    private String path;
    private long inodeId;
    private long mtime;
    private long atime;
    private int replication;
    private String ownerName;
    private String groupName;
    private String symlinkTarget;
    private List<AclEntry> acls;
    private List<XAttr> xAttrs;
    private boolean xAttrsRemoved;

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder inodeId(long inodeId) {
      this.inodeId = inodeId;
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

    public Builder symlinkTarget(String symlinkTarget) {
      this.symlinkTarget = symlinkTarget;
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
    this.inodeId = b.inodeId;
    this.mtime = b.mtime;
    this.atime = b.atime;
    this.replication = b.replication;
    this.ownerName = b.ownerName;
    this.groupName = b.groupName;
    this.symlinkTarget = b.symlinkTarget;
    this.acls = b.acls;
    this.xAttrs = b.xAttrs;
    this.xAttrsRemoved = b.xAttrsRemoved;
  }

  public String getPath() {
    return path;
  }

  public long getInodeId() {
    return inodeId;
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

  public String getSymlinkTarget() {
    return symlinkTarget;
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
