package org.apache.hadoop.hdfs.server.statestore;

import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.fs.permission.*;

public class INodeDirectoryMeta extends INodeMeta {
  public static final short TYPE = 1;
  public static INodeMeta[] EMPTY = new INodeMeta[0];
  public INodeMeta[] children;

  private int dirtyFlags;
  public static final int DIRTY_CHILDREN = 1 << 3;

  public INodeDirectoryMeta(long parentId, long id, byte[] name, long permission, long modificationTime, long accessTime) {
    super(parentId, id, name, permission, modificationTime, accessTime, 0, TYPE);
    this.children = EMPTY;
  }

  public INodeDirectoryMeta(INodeDirectory directory, int flags) {
    super(directory, 0, TYPE);
    this.dirtyFlags = flags;
  }

  public static INodeDirectory convert(INodeMeta meta, PermissionStatus ps) {
    return new INodeDirectory(meta.id,
        DFSUtil.string2Bytes(meta.name),
        ps,
        meta.modificationTime);
  }
}
