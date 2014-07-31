package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.fs.permission.AclEntry;

import java.util.List;

public class CreateEvent {

  public static enum INodeType {
    FILE, DIRECTORY, SYMLINK;
  }

  private INodeType type;
  private String path;
  private long inodeId;
  private long ctime;
  private int replication;
  private String ownerName;
  private String groupName;
  private String symlinkTarget;
  private List<AclEntry> acls;

  public static class Builder {
    private INodeType type;
    private String path;
    private long inodeId;
    private long ctime;
    private int replication;
    private String ownerName;
    private String groupName;
    private String symlinkTarget;
    private List<AclEntry> acls;

    public Builder type(INodeType type) {
      this.type = type;
      return this;
    }

    public Builder path(String path) {
      this.path = path;
      return this;
    }

    public Builder inodeId(long inodeId) {
      this.inodeId = inodeId;
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

    public Builder symlinkTarget(String symlinkTarget) {
      this.symlinkTarget = symlinkTarget;
      return this;
    }

    public Builder acls(List<AclEntry> acls) {
      this.acls = acls;
      return this;
    }

    public CreateEvent build() {
      return new CreateEvent(this);
    }
  }

  private CreateEvent(Builder b) {
    this.type = b.type;
    this.path = b.path;
    this.inodeId = b.inodeId;
    this.ctime = b.ctime;
    this.replication = b.replication;
    this.ownerName = b.ownerName;
    this.groupName = b.groupName;
    this.symlinkTarget = b.symlinkTarget;
    this.acls = b.acls;
  }

  public INodeType getType() {
    return type;
  }

  public String getPath() {
    return path;
  }

  public long getInodeId() {
    return inodeId;
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

  public String getSymlinkTarget() {
    return symlinkTarget;
  }

  public List<AclEntry> getAcls() {
    return acls;
  }
}
