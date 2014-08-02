package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.inotify.CloseEvent;
import org.apache.hadoop.hdfs.inotify.CreateEvent;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.MetadataUpdateEvent;
import org.apache.hadoop.hdfs.inotify.RenameEvent;
import org.apache.hadoop.hdfs.inotify.ReopenEvent;
import org.apache.hadoop.hdfs.inotify.UnlinkEvent;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.util.List;

public class InotifyFSEditLogOpTranslator {

  private static long getSize(FSEditLogOp.AddCloseOp acOp) {
    long size = 0;
    for (Block b : acOp.getBlocks()) {
      size += b.getNumBytes();
    }
    return size;
  }

  public static Event[] translate(FSEditLogOp op) {
    if (op instanceof FSEditLogOp.AddOp) {
      FSEditLogOp.AddOp addOp = (FSEditLogOp.AddOp) op;
      if (addOp.blocks.length == 0) { // create
        return new Event[]{new CreateEvent.Builder().path(addOp.path)
            .ctime(addOp.atime)
            .replication(addOp.replication)
            .ownerName(addOp.permissions.getUserName())
            .groupName(addOp.permissions.getGroupName())
            .perms(addOp.permissions.getPermission())
            .type(CreateEvent.INodeType.FILE).build()};
      } else {
        return new Event[]{new ReopenEvent(addOp.path)};
      }
    } else if (op instanceof FSEditLogOp.CloseOp) {
      FSEditLogOp.CloseOp cOp = (FSEditLogOp.CloseOp) op;
      return new Event[]{new CloseEvent(cOp.path, getSize(cOp))};
    } else if (op instanceof FSEditLogOp.SetReplicationOp) {
      FSEditLogOp.SetReplicationOp setRepOp = (FSEditLogOp.SetReplicationOp) op;
      return new Event[]{new MetadataUpdateEvent.Builder().path(setRepOp.path)
          .replication(setRepOp.replication).build()};
    } else if (op instanceof FSEditLogOp.ConcatDeleteOp) {
      FSEditLogOp.ConcatDeleteOp cdOp = (FSEditLogOp.ConcatDeleteOp) op;
      List<Event> events = Lists.newArrayList();
      events.add(new ReopenEvent(cdOp.trg));
      for (String src : cdOp.srcs) {
        events.add(new UnlinkEvent(src));
      }
      // TODO (james) we should do something better than returning -1 as size
      events.add(new CloseEvent(cdOp.trg, -1));
      return events.toArray(new Event[0]);
    } else if (op instanceof FSEditLogOp.RenameOldOp) {
      FSEditLogOp.RenameOldOp rnOp = (FSEditLogOp.RenameOldOp) op;
      return new Event[]{new RenameEvent(rnOp.src, rnOp.dst)};
    } else if (op instanceof FSEditLogOp.RenameOp) {
      FSEditLogOp.RenameOp rnOp = (FSEditLogOp.RenameOp) op;
      return new Event[]{new RenameEvent(rnOp.src, rnOp.dst)};
    } else if (op instanceof FSEditLogOp.DeleteOp) {
      return new Event[]{new UnlinkEvent(((FSEditLogOp.DeleteOp) op).path)};
    } else if (op instanceof FSEditLogOp.MkdirOp) {
      FSEditLogOp.MkdirOp mkOp = (FSEditLogOp.MkdirOp) op;
      return new Event[]{new CreateEvent.Builder().path(mkOp.path)
          .ctime(mkOp.timestamp)
          .ownerName(mkOp.permissions.getUserName())
          .groupName(mkOp.permissions.getGroupName())
          .perms(mkOp.permissions.getPermission())
          .type(CreateEvent.INodeType.DIRECTORY).build()};
    } else if (op instanceof FSEditLogOp.SetPermissionsOp) {
      FSEditLogOp.SetPermissionsOp permOp = (FSEditLogOp.SetPermissionsOp) op;
      return new Event[]{new MetadataUpdateEvent.Builder().path(permOp.src)
          .perms(permOp.permissions).build()};
    } else if (op instanceof FSEditLogOp.SetOwnerOp) {
      FSEditLogOp.SetOwnerOp ownOp = (FSEditLogOp.SetOwnerOp) op;
      return new Event[]{new MetadataUpdateEvent.Builder().path(ownOp.src)
          .ownerName(ownOp.username).groupName(ownOp.groupname).build()};
    } else if (op instanceof FSEditLogOp.TimesOp) {
      FSEditLogOp.TimesOp timesOp = (FSEditLogOp.TimesOp) op;
      return new Event[]{new MetadataUpdateEvent.Builder().path(timesOp.path)
          .atime(timesOp.atime).mtime(timesOp.mtime).build()};
    } else if (op instanceof FSEditLogOp.SymlinkOp) {
      FSEditLogOp.SymlinkOp symOp = (FSEditLogOp.SymlinkOp) op;
      return new Event[]{new CreateEvent.Builder().path(symOp.path)
          .ctime(symOp.atime)
          .ownerName(symOp.permissionStatus.getUserName())
          .groupName(symOp.permissionStatus.getGroupName())
          .perms(symOp.permissionStatus.getPermission())
          .symlinkTarget(symOp.value)
          .type(CreateEvent.INodeType.SYMLINK).build()};
    } else if (op instanceof FSEditLogOp.RemoveXAttrOp) {
      FSEditLogOp.RemoveXAttrOp rxOp = (FSEditLogOp.RemoveXAttrOp) op;
      return new Event[]{new MetadataUpdateEvent.Builder().path(rxOp.src)
          .xAttrs(rxOp.xAttrs)
          .xAttrsRemoved(true).build()};
    } else if (op instanceof FSEditLogOp.SetXAttrOp) {
      FSEditLogOp.SetXAttrOp sxOp = (FSEditLogOp.SetXAttrOp) op;
      return new Event[]{new MetadataUpdateEvent.Builder().path(sxOp.src)
          .xAttrs(sxOp.xAttrs)
          .xAttrsRemoved(false).build()};
    } else if (op instanceof FSEditLogOp.SetAclOp) {
      FSEditLogOp.SetAclOp saOp = (FSEditLogOp.SetAclOp) op;
      return new Event[]{new MetadataUpdateEvent.Builder().path(saOp.src)
          .acls(saOp.aclEntries).build()};
    } else {
      return null;
    }
  }
}
