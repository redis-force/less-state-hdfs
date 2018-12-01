package org.apache.hadoop.hdfs.server.statestore;

import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.permission.*;

public class INodeDirectoryMeta extends INodeMeta {
  public static short TYPE = 1;
  public static INodeMeta[] EMPTY = new INodeMeta[0];
  public INodeMeta[] children;

  public INodeDirectoryMeta(long parentId, long id, byte[] name, long permission, long modificationTime, long accessTime) {
    super(parentId, id, name, permission, modificationTime, accessTime, 0, TYPE);
    this.children = EMPTY;
  }

  public INodeDirectory convert(FSNamesystem namesystem) {
    return new INodeDirectory(id,
        DFSUtil.string2Bytes(name), 
        namesystem.createFsOwnerPermissions(new FsPermission(INodeWithAdditionalFields.getFsPermissionShort(permission))),
        modificationTime);
  }
}
